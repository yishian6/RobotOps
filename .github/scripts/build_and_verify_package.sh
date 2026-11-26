#!/usr/bin/env bash
set -euo pipefail

if (( $# != 1 )); then
  echo 'usage: build_and_verify_package.sh <artifact-directory>' >&2
  exit 2
fi

artifact_dir="$1"
uv build --no-sources --out-dir "$artifact_dir"
python3 .github/scripts/validate_artifacts.py "$artifact_dir"

work_dir="$(mktemp -d "${RUNNER_TEMP:-/tmp}/robotops-wheel.XXXXXXXX")"
trap 'rm -rf -- "$work_dir"' EXIT
venv_dir="$work_dir/venv"
UV_PROJECT_ENVIRONMENT="$venv_dir" \
  uv sync --locked --no-dev --no-install-project --python "$(cat .python-version)"
uv pip install --no-deps --python "$venv_dir/bin/python" "$artifact_dir/"*.whl

cd "$work_dir"
"$venv_dir/bin/robotops" check-prompts
"$venv_dir/bin/python" - <<'PY'
from importlib.metadata import version

from robotops import __version__
from robotops.agent.skills import load_incident_skill
from robotops.agent.supervision import load_role_prompts

load_role_prompts()
skill = load_incident_skill({"ros_fact": {"kind": "topic_startup_timeout"}})
if not skill or skill[1]["name"] != "ros1-topic-silence":
    raise SystemExit("The wheel is missing the bundled ROS 1 Skill.")
if version("robotops-agent") != __version__:
    raise SystemExit("Wheel metadata and runtime version differ.")
PY
