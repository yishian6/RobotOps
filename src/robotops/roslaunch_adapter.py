"""
    ROS 1 Noetic 的子进程退出适配器，由 roslaunch 所用的 Python 解释器独立运行。
"""

import json
import os
import socket
import sys


def launch(argv, events):
    """
        用 Noetic 的解析和启动逻辑加载现有 launch，额外上报进程退出。

        Args:
            argv: 原 roslaunch 命令在程序名之后的参数。
            events: 与 RobotOps 本次启动相连的私有事件套接字。

        Returns:
            None: launch 正常结束时回到调用方。
    """
    import roslaunch
    from roslaunch import parent, pmon, rlutil

    parser = roslaunch._get_optparse()
    options, launch_args = parser.parse_args(argv)
    if options.child_name:
        parser.error("run expects a top-level roslaunch command")
    launch_args = rlutil.resolve_launch_arguments(launch_args)
    roslaunch._validate_args(parser, options, launch_args)
    if any((
        options.node_args, options.node_list, options.find_node,
        options.dump_params, options.file_list, options.ros_args,
    )):
        roslaunch.main(["roslaunch", *argv])
        return

    def send_event(event):
        try:
            events.send(json.dumps(event, ensure_ascii=False).encode())
        except OSError as exc:
            # 事件上报失败时，ROS 继续它自己的启动和关停流程。
            print(f"[robotops] could not report ROS launch event: {exc}", file=sys.stderr, flush=True)

    class ExitListener(pmon.ProcessListener):
        """
            将 Noetic 主动报告的异常退出写到独立通道。
        """

        def __init__(self, monitor=None):
            self.monitor = monitor

        def process_died(self, process_name, exit_code):
            """
                发送一次明确的非零退出，保留 ROS 报告的名称和退出码。

                Args:
                    process_name: roslaunch 为实际进程分配的名称。
                    exit_code: ROS 记录的退出码，未知时为 None。

                Returns:
                    None: 事件独立于节点的任意文本输出发送。
            """
            if exit_code is not None and exit_code != 0:
                process = self.monitor.get_process(process_name) if self.monitor is not None else None
                send_event({
                    "type": "exit", "name": process_name[:1024], "code": exit_code,
                    "required": process is not None and process.required,
                })

    class ObservedLaunch(parent.ROSLaunchParent):
        """
            在实际启动节点之前安装监听器，覆盖启动后立刻崩溃的节点。
        """

        def _init_runner(self):
            """
                按 Noetic 原顺序创建 runner，再在 launch() 之前接好监听器。

                Returns:
                    None: 退出监听器已注册到本地进程监控和远程回调入口。
            """
            super()._init_runner()
            self.pm.add_process_listener(ExitListener(self.pm))
            self.server.add_process_listener(ExitListener())

    if options.wait_for_master and not options.core:
        rlutil._wait_for_master()
    run_id = rlutil.get_or_generate_uuid(options.run_id, options.wait_for_master)
    roslaunch.configure_logging(run_id)
    if not options.skip_log_check:
        rlutil.check_log_disk_usage()
    launch_xml = []
    if "-" in launch_args:
        launch_xml.append(sys.stdin.read())
        launch_args.remove("-")
    # 使用原生 ROSLaunchParent 保持条件、include、参数、respawn 和关停语义。
    launcher = ObservedLaunch(
        run_id, launch_args, roslaunch_strs=launch_xml,
        is_core=options.core, port=(options.port or rlutil.DEFAULT_MASTER_PORT)
        if options.core else options.port, local_only=options.local_only,
        verbose=options.verbose, force_screen=options.force_screen,
        force_log=options.force_log, num_workers=options.num_workers,
        timeout=options.timeout, master_logger_level=options.master_logger_level,
        show_summary=not options.no_summary, force_required=options.force_required,
        sigint_timeout=options.sigint_timeout, sigterm_timeout=options.sigterm_timeout,
    )
    roslaunch.write_pid_file(options.pid_fn, options.core, options.port)
    try:
        launcher.start()
        send_event({"type": "started", "master_uri": launcher.config.master.uri})
        launcher.spin()
    finally:
        launcher.shutdown()
        if options.pid_fn:
            # pid 文件属于本次 roslaunch 命令，沿用其退出后的清理约定。
            try:
                os.unlink(options.pid_fn)
            except FileNotFoundError:
                pass


if __name__ == "__main__":
    with socket.socket(fileno=int(sys.argv[1])) as channel:
        # 事件通道仅供适配器使用，ROS 启动的节点只继承应该收到的终端 I/O。
        channel.set_inheritable(False)
        launch(sys.argv[2:], channel)
