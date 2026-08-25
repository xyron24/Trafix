#include "observer.h"
#include "core/socket_utils.h"
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <cstring>
#include <cstdio>
#include <cerrno>

GlobalMetrics g_metrics;
ObservabilityWorker* g_observer = nullptr;

static constexpr int MAX_EPOLL_EVENTS = 16;
static constexpr int POLL_TIMEOUT_MS = 100;

static const char* event_type_to_string(EventType type);

static void replace_all(std::string& str, const std::string& from, const std::string& to) {
    if (from.empty()) return;
    size_t start_pos = 0;
    while ((start_pos = str.find(from, start_pos)) != std::string::npos) {
        str.replace(start_pos, from.length(), to);
        start_pos += to.length(); 
    }
}

ObservabilityWorker::ObservabilityWorker(const std::string& socket_path)
    : socket_path_(socket_path), epoll_fd_(INVALID_FD), admin_fd_(INVALID_FD), running_(false) {
    event_history_.resize(MAX_EVENTS);
}

ObservabilityWorker::~ObservabilityWorker() {
    stop();
    if (admin_fd_ != INVALID_FD) {
        close_fd(admin_fd_);
        unlink(socket_path_.c_str());
    }
    if (epoll_fd_ != INVALID_FD) {
        close_fd(epoll_fd_);
    }
}

bool ObservabilityWorker::init() {
    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ == INVALID_FD) {
        std::fprintf(stderr, "observer: epoll_create1 failed: %s\n", std::strerror(errno));
        return false;
    }

    unlink(socket_path_.c_str());
    admin_fd_ = socket(AF_UNIX, SOCK_STREAM, 0);
    if (admin_fd_ == INVALID_FD) {
        std::fprintf(stderr, "observer: socket AF_UNIX failed: %s\n", std::strerror(errno));
        return false;
    }

    struct sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    std::strncpy(addr.sun_path, socket_path_.c_str(), sizeof(addr.sun_path) - 1);

    if (bind(admin_fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        std::fprintf(stderr, "observer: bind AF_UNIX failed: %s\n", std::strerror(errno));
        return false;
    }

    if (listen(admin_fd_, 128) < 0) {
        std::fprintf(stderr, "observer: listen AF_UNIX failed: %s\n", std::strerror(errno));
        return false;
    }

    set_nonblocking(admin_fd_);

    struct epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = admin_fd_;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, admin_fd_, &ev) < 0) {
        std::fprintf(stderr, "observer: epoll_ctl add admin_fd failed: %s\n", std::strerror(errno));
        return false;
    }

    return true;
}

void ObservabilityWorker::run() {
    running_ = true;
    struct epoll_event events[MAX_EPOLL_EVENTS];

    while (running_.load()) {
        int n = epoll_wait(epoll_fd_, events, MAX_EPOLL_EVENTS, POLL_TIMEOUT_MS);
        if (n < 0) {
            if (errno == EINTR) continue;
            std::fprintf(stderr, "observer: epoll_wait error: %s\n", std::strerror(errno));
            continue;
        }

        consume_events();

        for (int i = 0; i < n; ++i) {
            fd_t fd = events[i].data.fd;
            if (fd == admin_fd_) {
                handle_admin_accept();
            } else {
                handle_admin_request(fd);
            }
        }
    }
}

void ObservabilityWorker::stop() {
    running_.store(false);
}

EventQueue* ObservabilityWorker::register_thread() {
    std::lock_guard<std::mutex> lock(queues_mutex_);
    thread_queues_.push_back(std::make_unique<EventQueue>());
    return thread_queues_.back().get();
}

void ObservabilityWorker::record_event(EventQueue* queue, EventType type, fd_t client_fd, fd_t backend_fd, const std::string& metadata) {
    if (!queue) return;
    
    EventRecord record;
    record.timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    record.type = type;
    record.client_fd = client_fd;
    record.backend_fd = backend_fd;
    record.metadata = metadata;

    queue->push(record);
}

void ObservabilityWorker::consume_events() {
    const bool use_color = (isatty(fileno(stdout)) != 0);

    std::lock_guard<std::mutex> lock(queues_mutex_);
    for (auto& queue : thread_queues_) {
        EventRecord record;
        int count = 0;
        while (count++ < 100 && queue->pop(record)) {
            event_history_[history_head_] = record;
            history_head_ = (history_head_ + 1) % MAX_EVENTS;
            if (history_count_ < MAX_EVENTS) {
                history_count_++;
            }
            
            if (record.type != EventType::SYSTEM_LOG || !record.metadata.empty()) {
                std::string full_msg = "[OBSERVABILITY][" + std::string(event_type_to_string(record.type)) + "] " + record.metadata;
                
                if (use_color) {
                    replace_all(full_msg, "[OBSERVABILITY]", "\x1b[1;33m[OBSERVABILITY]\x1b[0m");
                    replace_all(full_msg, "[CLIENT_CONNECTED]", "\x1b[1;32m[CLIENT_CONNECTED]\x1b[0m");
                    replace_all(full_msg, "[CLIENT_DISCONNECTED]", "\x1b[1;31m[CLIENT_DISCONNECTED]\x1b[0m");
                    replace_all(full_msg, "[LOAD_BALANCER]", "\x1b[1;34m[LOAD_BALANCER]\x1b[0m");
                    replace_all(full_msg, "[HEALTH_CHECKER]", "\x1b[1;35m[HEALTH_CHECKER]\x1b[0m");
                }
                
                std::printf("%s\n", full_msg.c_str());
                std::fflush(stdout);
            }
        }
    }
}

void ObservabilityWorker::handle_admin_accept() {
    while (true) {
        struct sockaddr_un client_addr{};
        socklen_t client_len = sizeof(client_addr);
        fd_t client_fd = accept(admin_fd_, reinterpret_cast<struct sockaddr*>(&client_addr), &client_len);
        
        if (client_fd == INVALID_FD) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            break;
        }
        
        set_nonblocking(client_fd);
        
        struct epoll_event ev{};
        ev.events = EPOLLIN | EPOLLRDHUP;
        ev.data.fd = client_fd;
        epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client_fd, &ev);
    }
}

void ObservabilityWorker::handle_admin_request(fd_t client_fd) {
    char buf[1024];
    ssize_t n = read(client_fd, buf, sizeof(buf) - 1);
    
    if (n <= 0) {
        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, client_fd, nullptr);
        close_fd(client_fd);
        return;
    }
    
    buf[n] = '\0';
    std::string req(buf);
    std::string response;
    
    if (req.find("GET_METRICS") != std::string::npos) {
        response = generate_metrics_json();
    } else if (req.find("GET_EVENTS") != std::string::npos) {
        response = generate_events_json();
    } else if (req.find("GET_TOPOLOGY") != std::string::npos) {
        response = generate_topology_json();
    } else {
        response = "{\"error\": \"unknown command\"}\n";
    }
    
    write(client_fd, response.c_str(), response.length());
    
    epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, client_fd, nullptr);
    close_fd(client_fd);
}

static const char* event_type_to_string(EventType type) {
    if (type == EventType::CLIENT_CONNECTED) return "CLIENT_CONNECTED";
    if (type == EventType::CLIENT_DISCONNECTED) return "CLIENT_DISCONNECTED";
    if (type == EventType::BACKEND_ERROR) return "BACKEND_ERROR";
    if (type == EventType::HEALTH_STATE_CHANGED) return "HEALTH_CHECKER";
    if (type == EventType::SYSTEM_LOG) return "SYSTEM_LOG";
    if (type == EventType::LOAD_BALANCER) return "LOAD_BALANCER";
    return "UNKNOWN";
}

std::string ObservabilityWorker::generate_metrics_json() {
    char buf[512];
    std::snprintf(buf, sizeof(buf),
        "{\"total_connections\": %lu, \"active_connections\": %lu, \"bytes_c2b\": %lu, \"bytes_b2c\": %lu}\n",
        g_metrics.total_connections.load(),
        g_metrics.active_connections.load(),
        g_metrics.bytes_c2b.load(),
        g_metrics.bytes_b2c.load());
    return std::string(buf);
}

std::string ObservabilityWorker::generate_events_json() {
    std::string json = "[";
    size_t start_idx = (history_count_ < MAX_EVENTS) ? 0 : history_head_;
    
    for (size_t i = 0; i < history_count_; ++i) {
        size_t idx = (start_idx + i) % MAX_EVENTS;
        const auto& ev = event_history_[idx];
        
        char buf[256];
        std::snprintf(buf, sizeof(buf),
            "{\"timestamp\": %lu, \"type\": \"%s\", \"client_fd\": %d, \"backend_fd\": %d, \"metadata\": \"%s\"}",
            ev.timestamp_ms, event_type_to_string(ev.type), ev.client_fd, ev.backend_fd, ev.metadata.c_str());
            
        json += buf;
        if (i < history_count_ - 1) {
            json += ",";
        }
    }
    json += "]\n";
    return json;
}

void ObservabilityWorker::update_router(const Router& new_router) {
    std::lock_guard<std::mutex> lock(router_mutex_);
    router_ = new_router;
}

std::string ObservabilityWorker::generate_topology_json() {
    std::lock_guard<std::mutex> lock(router_mutex_);
    std::string json = "{\"services\": [";
    bool first_service = true;
    for (const auto& entry : router_.get_routes()) {
        if (!first_service) json += ",";
        first_service = false;
        const ServiceTarget& target = entry.second;
        json += "{\"name\": \"" + target.name + "\", \"listen_port\": " + std::to_string(entry.first) + ", \"backends\": [";
        bool first_backend = true;
        for (const auto& backend : target.backends) {
            if (!first_backend) json += ",";
            first_backend = false;
            json += "{\"host\": \"" + backend->host + "\", \"port\": " + std::to_string(backend->port) + ", \"is_healthy\": " + (backend->is_healthy.load() ? "true" : "false") + ", \"active_connections\": " + std::to_string(backend->active_connections.load()) + "}";
        }
        json += "]}";
    }
    json += "]}\n";
    return json;
}
