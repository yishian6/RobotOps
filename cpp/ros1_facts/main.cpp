#include <boost/asio.hpp>
#include <boost/beast.hpp>
#include <fcntl.h>
#include <nlohmann/json.hpp>
#include <ros/ros.h>
#include <sqlite3.h>
#include <sys/file.h>
#include <topic_tools/shape_shifter.h>
#include <unistd.h>

#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <regex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

namespace {
namespace asio = boost::asio;
namespace http = boost::beast::http;
using asio::ip::tcp;
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
    FactDatabase(const std::string& path, const std::string& app, const std::string& topic) {
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

    void record(std::int64_t last_message_at_ms, Milliseconds silence, Milliseconds threshold) {
        Statement row(database_.get(), R"sql(
            INSERT INTO topic_silences (observed_at_ms, last_message_at_ms, silent_ms, threshold_ms)
            VALUES (?, ?, ?, ?)
        )sql");
        row.bind(1, now_ms());
        row.bind(2, last_message_at_ms);
        row.bind(3, silence.count());
        row.bind(4, threshold.count());
        row.next();
    }

    const Json& source() const { return source_; }

    Json after(std::int64_t sequence) {
        Statement rows(database_.get(), R"sql(
            SELECT * FROM topic_silences WHERE sequence > ? ORDER BY sequence LIMIT 100
        )sql");
        rows.bind(1, sequence);
        Json response = source_;
        response["events"] = Json::array();
        while (rows.next()) {
            response["events"].push_back(fact(rows));
        }
        return response;
    }

    std::optional<Json> event(std::int64_t sequence) {
        Statement row(database_.get(), "SELECT * FROM topic_silences WHERE sequence=?");
        row.bind(1, sequence);
        if (!row.next()) {
            return std::nullopt;
        }
        Json result = source_;
        result.update(fact(row));
        return result;
    }

private:
    static Json fact(const Statement& row) {
        return {
            {"sequence", row.number(0)}, {"kind", "topic_silent"},
            {"observed_at_ms", row.number(1)}, {"last_message_at_ms", row.number(2)},
            {"silent_ms", row.number(3)}, {"threshold_ms", row.number(4)}
        };
    }

    std::unique_ptr<sqlite3, decltype(&sqlite3_close)> database_{nullptr, sqlite3_close};
    Json source_;
};

class TopicObserver {
public:
    TopicObserver(ros::NodeHandle& node, FactDatabase& database, const std::string& topic,
                  Milliseconds threshold)
        : database_(database), threshold_(threshold),
          subscriber_(node.subscribe(topic, 1, &TopicObserver::receive, this)) {}

    void poll() {
        if (!last_message_ || recorded_) {
            return;
        }
        const auto silence = std::chrono::duration_cast<Milliseconds>(
            SteadyClock::now() - *last_message_);
        if (silence >= threshold_) {
            database_.record(last_message_at_ms_, silence, threshold_);
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
    Milliseconds threshold_;
    std::optional<SteadyClock::time_point> last_message_;
    std::int64_t last_message_at_ms_ = 0;
    bool recorded_ = false;
    ros::Subscriber subscriber_;
};

using Response = http::response<http::string_body>;

Response route(FactDatabase& database, const http::request<http::empty_body>& request) {
    Response response{http::status::ok, request.version()};
    response.set(http::field::content_type, "application/json");
    response.keep_alive(false);
    std::string_view path(request.target().data(), request.target().size());
    constexpr std::string_view list_prefix = "/v1/events/";
    constexpr std::string_view item_prefix = "/v1/event/";
    try {
        if (request.method() != http::verb::get) {
            response.result(http::status::method_not_allowed);
            response.set(http::field::allow, "GET");
        } else if (path == "/v1/source") {
            response.body() = database.source().dump();
        } else if (path.substr(0, list_prefix.size()) == list_prefix) {
            response.body() = database.after(integer(path.substr(list_prefix.size()))).dump();
        } else if (path.substr(0, item_prefix.size()) == item_prefix) {
            if (auto event = database.event(integer(path.substr(item_prefix.size())))) {
                response.body() = event->dump();
            } else {
                response.result(http::status::not_found);
            }
        } else {
            response.result(http::status::not_found);
        }
    } catch (const std::invalid_argument&) {
        response.result(http::status::bad_request);
    } catch (const std::exception& error) {
        ROS_ERROR_STREAM("Unable to read facts: " << error.what());
        response.result(http::status::internal_server_error);
    }
    if (response.body().empty()) {
        response.body() = Json{{"error", std::string(response.reason())}}.dump();
    }
    response.prepare_payload();
    return response;
}

class Session : public std::enable_shared_from_this<Session> {
public:
    Session(tcp::socket socket, FactDatabase& database)
        : stream_(std::move(socket)), database_(database) {}

    void start() {
        request_.header_limit(4096);
        request_.body_limit(0);
        stream_.expires_after(std::chrono::seconds(2));
        http::async_read(stream_, buffer_, request_,
            [self = shared_from_this()](boost::system::error_code error, std::size_t) {
                if (!error) {
                    self->send();
                }
            });
    }

private:
    void send() {
        response_ = route(database_, request_.get());
        stream_.expires_after(std::chrono::seconds(2));
        http::async_write(stream_, response_,
            [self = shared_from_this()](boost::system::error_code, std::size_t) {});
    }

    boost::beast::tcp_stream stream_;
    FactDatabase& database_;
    boost::beast::flat_buffer buffer_;
    http::request_parser<http::empty_body> request_;
    Response response_;
};

class Server {
public:
    Server(FactDatabase& database, unsigned short port)
        : database_(database),
          acceptor_(io_, {asio::ip::address_v4::loopback(), port}, true) {
        accept();
    }

    void poll() { io_.run_for(Milliseconds(20)); }

private:
    void accept() {
        acceptor_.async_accept([this](boost::system::error_code error, tcp::socket socket) {
            if (!error) {
                std::make_shared<Session>(std::move(socket), database_)->start();
            }
            accept();
        });
    }

    FactDatabase& database_;
    asio::io_context io_;
    tcp::acceptor acceptor_;
};
}  // namespace

int main(int argc, char** argv) {
    ros::init(argc, argv, "robotops_ros1_facts", ros::init_options::AnonymousName);
    if (argc != 6) {
        std::cerr << "Usage: robotops_ros1_facts APP_ID /TOPIC SILENCE_MS PORT DATABASE\n";
        return 2;
    }

    try {
        const std::string app = argv[1];
        const std::string topic = argv[2];
        const auto threshold = integer(argv[3]);
        const auto port = integer(argv[4]);
        const std::string database_path = argv[5];
        if (!std::regex_match(app, std::regex("[a-z][a-z0-9_-]{0,63}")) ||
            topic.size() > 256 || !std::regex_match(topic, std::regex("/[A-Za-z0-9_/]+")) ||
            threshold < 100 || threshold > 86400000 || port == 0 || port > 65535) {
            throw std::invalid_argument("Invalid app, Topic, silence threshold, or port");
        }
        if (database_path.empty() || database_path == ":memory:" ||
            std::filesystem::path(database_path).filename() == "runtime.db") {
            throw std::invalid_argument("Provide a separate persistent ROS facts database");
        }
        CollectorLock lock(database_path);
        FactDatabase database(database_path, app, topic);
        ros::NodeHandle node;
        TopicObserver observer(node, database, topic, Milliseconds(threshold));
        Server server(database, static_cast<unsigned short>(port));
        while (ros::ok()) {
            ros::spinOnce();
            observer.poll();
            server.poll();
        }
    } catch (const std::exception& error) {
        std::cerr << "ROS 1 fact collector: " << error.what() << '\n';
        return 1;
    }
}
