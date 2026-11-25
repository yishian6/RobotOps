#include <fcntl.h>
#include <nlohmann/json.hpp>
#include <ros/callback_queue.h>
#include <ros/ros.h>
#include <sqlite3.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <topic_tools/shape_shifter.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <regex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>

namespace {
using Json = nlohmann::json;
using Milliseconds = std::chrono::milliseconds;
using SteadyClock = std::chrono::steady_clock;
using WallClock = std::chrono::system_clock;

std::int64_t integer(std::string_view text) {
    std::int64_t value = 0;
    const auto result = std::from_chars(text.data(), text.data() + text.size(), value);
    if (result.ec != std::errc{} || result.ptr != text.data() + text.size() || value < 0) {
        throw std::invalid_argument("Expected a nonnegative integer");
    }
    return value;
}

std::int64_t now_ms() {
    return std::chrono::duration_cast<Milliseconds>(WallClock::now().time_since_epoch()).count();
}

bool valid_topic(const std::string& topic) {
    return topic.size() <= 256 && std::regex_match(topic, std::regex("/[A-Za-z0-9_/]+"));
}

void check(sqlite3* database, int result) {
    if (result != SQLITE_OK) {
        throw std::runtime_error(sqlite3_errmsg(database));
    }
}

class Statement {
public:
    Statement(sqlite3* database, const char* sql) : database_(database) {
        sqlite3_stmt* handle = nullptr;
        check(database_, sqlite3_prepare_v2(database_, sql, -1, &handle, nullptr));
        statement_.reset(handle);
    }

    void bind(int column, std::int64_t value) {
        check(database_, sqlite3_bind_int64(statement_.get(), column, value));
    }

    void bind(int column, const std::string& value) {
        check(database_, sqlite3_bind_text(
            statement_.get(), column, value.c_str(), -1, SQLITE_TRANSIENT));
    }

    bool next() {
        const auto result = sqlite3_step(statement_.get());
        if (result == SQLITE_ROW) {
            return true;
        }
        if (result != SQLITE_DONE) {
            check(database_, result);
        }
        return false;
    }

    std::int64_t number(int column) const { return sqlite3_column_int64(statement_.get(), column); }

    std::string text(int column) const {
        return reinterpret_cast<const char*>(sqlite3_column_text(statement_.get(), column));
    }

private:
    sqlite3* database_;
    std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> statement_{nullptr, sqlite3_finalize};
};

class CollectorLock {
public:
    explicit CollectorLock(const std::string& path) {
        const auto lock_path = std::filesystem::weakly_canonical(path).string() + ".lock";
        handle_ = open(lock_path.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0600);
        if (handle_ < 0) {
            throw std::system_error(errno, std::generic_category(), "Open fact database lock");
        }
        if (flock(handle_, LOCK_EX | LOCK_NB) != 0) {
            const auto error = errno;
            close(handle_);
            throw std::system_error(error, std::generic_category(), "Acquire fact database lock");
        }
    }

    ~CollectorLock() { close(handle_); }
    CollectorLock(const CollectorLock&) = delete;
    CollectorLock& operator=(const CollectorLock&) = delete;

private:
    int handle_;
};

class FactDatabase {
public:
    explicit FactDatabase(const std::string& path) {
        if (path.empty() || path == ":memory:" ||
            std::filesystem::path(path).filename() == "runtime.db") {
            throw std::invalid_argument("Provide a separate persistent ROS facts database");
        }
        sqlite3* handle = nullptr;
        const auto opened = sqlite3_open(path.c_str(), &handle);
        database_.reset(handle);
        if (!handle) {
            throw std::runtime_error("Unable to allocate SQLite connection");
        }
        check(handle, opened);
        check(handle, sqlite3_busy_timeout(handle, 2000));

        // An existing database must belong to this service, even when the filename is different.
        {
            Statement info(handle, "PRAGMA application_id");
            info.next();
            if (info.number(0) != 0x524F5346) {
                Statement tables(handle, "SELECT count(*) FROM sqlite_master");
                tables.next();
                if (info.number(0) != 0 || tables.number(0) != 0) {
                    throw std::invalid_argument("Database belongs to another application");
                }
            }
        }

        check(handle, sqlite3_exec(handle, R"sql(
            PRAGMA journal_mode=WAL;
            BEGIN IMMEDIATE;
            PRAGMA application_id=1380930374;
            CREATE TABLE IF NOT EXISTS source (
                slot INTEGER PRIMARY KEY CHECK (slot = 1),
                id TEXT NOT NULL, app TEXT NOT NULL, topic TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS topic_silences (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at_ms INTEGER NOT NULL, last_message_at_ms INTEGER NOT NULL,
                silent_ms INTEGER NOT NULL, threshold_ms INTEGER NOT NULL
            );
            COMMIT;
        )sql", nullptr, nullptr, nullptr));

    }

    void bind_source(const std::string& app, const std::string& topic) {
        auto* handle = database_.get();
        Statement create_source(
            handle, "INSERT OR IGNORE INTO source VALUES (1, lower(hex(randomblob(16))), ?, ?)");
        create_source.bind(1, app);
        create_source.bind(2, topic);
        create_source.next();

        Statement source(handle, "SELECT id, app, topic FROM source WHERE slot=1");
        if (!source.next() || source.text(1) != app || source.text(2) != topic) {
            throw std::invalid_argument("Facts database is already bound to another app or Topic");
        }
        source_ = {{"source_id", source.text(0)}, {"app_id", app}, {"topic", topic}};
    }

    Json record(std::int64_t last_message_at_ms, Milliseconds silence, Milliseconds threshold) {
        const auto observed_at_ms = now_ms();
        Statement row(database_.get(), R"sql(
            INSERT INTO topic_silences (observed_at_ms, last_message_at_ms, silent_ms, threshold_ms)
            VALUES (?, ?, ?, ?)
        )sql");
        row.bind(1, observed_at_ms);
        row.bind(2, last_message_at_ms);
        row.bind(3, silence.count());
        row.bind(4, threshold.count());
        row.next();

        Json fact = source_;
        fact.update({
            {"sequence", sqlite3_last_insert_rowid(database_.get())}, {"kind", "topic_silent"},
            {"observed_at_ms", observed_at_ms}, {"last_message_at_ms", last_message_at_ms},
            {"silent_ms", silence.count()}, {"threshold_ms", threshold.count()},
        });
        return fact;
    }

private:
    std::unique_ptr<sqlite3, decltype(&sqlite3_close)> database_{nullptr, sqlite3_close};
    Json source_;
};

class FactPublisher {
public:
    explicit FactPublisher(const std::string& path) {
        if (path.empty() || path.size() >= sizeof(address_.sun_path)) {
            throw std::invalid_argument("Invalid Unix socket path");
        }
        address_.sun_family = AF_UNIX;
        std::memcpy(address_.sun_path, path.c_str(), path.size() + 1);
        handle_ = socket(AF_UNIX, SOCK_DGRAM | SOCK_CLOEXEC, 0);
        if (handle_ < 0) {
            throw std::system_error(errno, std::generic_category(), "Open ROS fact socket");
        }
    }

    ~FactPublisher() { close(handle_); }
    FactPublisher(const FactPublisher&) = delete;
    FactPublisher& operator=(const FactPublisher&) = delete;

    void publish(const Json& fact) const {
        const auto body = fact.dump();
        if (sendto(handle_, body.data(), body.size(), MSG_DONTWAIT,
                   reinterpret_cast<const sockaddr*>(&address_), sizeof(address_)) < 0) {
            ROS_WARN_STREAM("ROS fact " << fact["sequence"] << " recorded locally, push failed: "
                            << std::strerror(errno));
        }
    }

private:
    sockaddr_un address_{};
    int handle_;
};

class TopicObserver {
public:
    TopicObserver(ros::NodeHandle& node, FactDatabase& database, const FactPublisher& publisher,
                  const std::string& topic, Milliseconds threshold)
        : database_(database), publisher_(publisher), threshold_(threshold),
          subscriber_(node.subscribe(topic, 1, &TopicObserver::receive, this)) {}

    void poll() {
        if (!last_message_ || recorded_) {
            return;
        }
        const auto silence = std::chrono::duration_cast<Milliseconds>(
            SteadyClock::now() - *last_message_);
        if (silence >= threshold_) {
            publisher_.publish(database_.record(last_message_at_ms_, silence, threshold_));
            recorded_ = true;
        }
    }

private:
    void receive(const topic_tools::ShapeShifter::ConstPtr&) {
        last_message_ = SteadyClock::now();
        last_message_at_ms_ = now_ms();
        recorded_ = false;
    }

    FactDatabase& database_;
    const FactPublisher& publisher_;
    Milliseconds threshold_;
    std::optional<SteadyClock::time_point> last_message_;
    std::int64_t last_message_at_ms_ = 0;
    bool recorded_ = false;
    ros::Subscriber subscriber_;
};

class TopicAcceptance {
public:
    TopicAcceptance(Milliseconds max_gap, Milliseconds window, Milliseconds timeout)
        : max_gap_(max_gap), window_(window), deadline_(SteadyClock::now() + timeout) {}

    void receive(const ros::MessageEvent<const topic_tools::ShapeShifter>& event) {
        const auto now = SteadyClock::now();
        if (now >= deadline_ || passed_) {
            return;
        }
        const auto& header = event.getConnectionHeaderPtr();
        if (!header) {
            return;
        }
        const auto caller = header->find("callerid");
        if (caller == header->end() || caller->second.empty()) {
            return;
        }
        const auto& publisher = caller->second;
        const auto latch = header->find("latching");
        if (latch != header->end() && latch->second == "1") {
            auto& previous = latched_connections_[publisher];
            if (previous != header) {
                // The replay on a new latched connection cannot establish new output.
                previous = header;
                return;
            }
        }
        const auto wall_ms = now_ms();
        const auto gap = now - last_;
        if (window_messages_ == 0 || publisher != publisher_ || gap > max_gap_) {
            first_ = now;
            first_ms_ = wall_ms;
            publisher_ = publisher;
            max_gap_seen_ = SteadyClock::duration::zero();
            window_messages_ = 0;
        } else {
            max_gap_seen_ = std::max(max_gap_seen_, gap);
        }
        ++messages_;
        ++window_messages_;
        last_ = now;
        last_ms_ = wall_ms;
        passed_ = now - first_ >= window_;
    }

    bool observing() const { return !passed_ && SteadyClock::now() < deadline_; }
    bool passed() const { return passed_; }

    Json observation() const {
        return {
            {"messages_observed", messages_}, {"window_messages", window_messages_},
            {"window_elapsed_ms", std::chrono::duration_cast<Milliseconds>(last_ - first_).count()},
            {"window_max_gap_ms", window_messages_ > 1
                ? Json(std::chrono::duration_cast<Milliseconds>(max_gap_seen_).count()) : Json(nullptr)},
            {"publisher", messages_ ? Json(publisher_) : Json(nullptr)},
            {"first_message_at_ms", messages_ ? Json(first_ms_) : Json(nullptr)},
            {"last_message_at_ms", messages_ ? Json(last_ms_) : Json(nullptr)},
        };
    }

private:
    Milliseconds max_gap_, window_;
    SteadyClock::time_point deadline_, first_{}, last_{};
    SteadyClock::duration max_gap_seen_{};
    std::string publisher_;
    std::map<std::string, boost::shared_ptr<ros::M_string>> latched_connections_;
    std::int64_t messages_ = 0, window_messages_ = 0, first_ms_ = 0, last_ms_ = 0;
    bool passed_ = false;
};

int verify_topic(const std::string& topic, std::int64_t gap, std::int64_t window, std::int64_t timeout) {
    if (!valid_topic(topic) || gap < 1 || gap > 86400000 ||
        window <= gap || timeout <= window || timeout > 604800000) {
        throw std::invalid_argument("Invalid Topic verification arguments");
    }
    TopicAcceptance check{Milliseconds(gap), Milliseconds(window), Milliseconds(timeout)};
    ros::CallbackQueue queue;
    ros::NodeHandle node;
    node.setCallbackQueue(&queue);
    auto subscription = node.subscribe(topic, 1, &TopicAcceptance::receive, &check);
    while (ros::ok() && check.observing()) {
        queue.callOne(ros::WallDuration(0.02));
    }
    if (!ros::ok()) {
        return 1;
    }
    std::cout << check.observation().dump() << std::endl;
    return check.passed() ? 0 : 3;
}

}  // namespace

int main(int argc, char** argv) {
    try {
        if (argc == 3 && std::string_view(argv[1]) == "--init-db") {
            CollectorLock lock(argv[2]);
            FactDatabase database(argv[2]);
            return 0;
        }
        ros::init(argc, argv, "robotops_ros1_facts", ros::init_options::AnonymousName);
        if (argc == 6 && std::string_view(argv[1]) == "--verify-topic") {
            return verify_topic(argv[2], integer(argv[3]), integer(argv[4]), integer(argv[5]));
        }
        if (argc != 6) {
            std::cerr << "Usage: robotops_ros1_facts APP_ID /TOPIC SILENCE_MS SOCKET DATABASE\n"
                         "       robotops_ros1_facts --init-db DATABASE\n"
                         "       robotops_ros1_facts --verify-topic /TOPIC GAP_MS WINDOW_MS TIMEOUT_MS\n";
            return 2;
        }

        const std::string app = argv[1];
        const std::string topic = argv[2];
        const auto threshold = integer(argv[3]);
        const std::string database_path = argv[5];
        if (!std::regex_match(app, std::regex("[a-z][a-z0-9_-]{0,63}")) ||
            !valid_topic(topic) ||
            threshold < 100 || threshold > 86400000) {
            throw std::invalid_argument("Invalid app, Topic, or silence threshold");
        }
        CollectorLock lock(database_path);
        FactDatabase database(database_path);
        database.bind_source(app, topic);
        FactPublisher publisher(argv[4]);
        ros::NodeHandle node;
        TopicObserver observer(node, database, publisher, topic, Milliseconds(threshold));
        ros::WallRate rate(50);
        while (ros::ok()) {
            ros::spinOnce();
            observer.poll();
            rate.sleep();
        }
    } catch (const std::exception& error) {
        std::cerr << "ROS 1 fact collector: " << error.what() << '\n';
        return 1;
    }
}
