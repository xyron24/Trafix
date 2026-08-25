#include "health_checker.h"
#include "common/types.h"
#include <sys/timerfd.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <cstdio>
#include "observability/observer.h"

static constexpr int MAX_EVENTS = 16;

HealthChecker::HealthChecker(const Router& router, int interval_seconds)
    : router_(router), interval_seconds_(interval_seconds), epoll_fd_(INVALID_FD), timer_fd_(INVALID_FD), running_(false), obs_queue_(nullptr) {}

HealthChecker::~HealthChecker() {
    if (timer_fd_ != INVALID_FD) {
        close(timer_fd_);
    }
    if (epoll_fd_ != INVALID_FD) {
        close(epoll_fd_);
    }
}

bool HealthChecker::init() {
    if (g_observer) {
        obs_queue_ = g_observer->register_thread();
    }
    
    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ == INVALID_FD) {
        return false;
    }

    timer_fd_ = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    if (timer_fd_ == INVALID_FD) {
        return false;
    }

    struct itimerspec ts{};
    ts.it_value.tv_sec = interval_seconds_;
    ts.it_value.tv_nsec = 0;
    ts.it_interval.tv_sec = interval_seconds_;
    ts.it_interval.tv_nsec = 0;
    
    if (timerfd_settime(timer_fd_, 0, &ts, nullptr) < 0) {
        return false;
    }

    struct epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = timer_fd_;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, timer_fd_, &ev) < 0) {
        return false;
    }

    // Initial Connection Phase
    for (const auto& pair : router_.get_routes()) {
        const ServiceTarget& target = pair.second;
        for (const auto& backend : target.backends) {
            fd_t sock = try_connect(backend->host, backend->port);
            if (sock != INVALID_FD) {
                backend->is_healthy.store(true);
                struct epoll_event bev{};
                bev.events = EPOLLIN | EPOLLRDHUP;
                bev.data.fd = sock;
                epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, sock, &bev);
                watch_sockets_[sock] = backend;
            } else {
                backend->is_healthy.store(false);
            }
        }
    }

    return true;
}

void HealthChecker::run() {
    running_ = true;
    struct epoll_event events[MAX_EVENTS];
    while (running_.load()) {
        if (reload_pending_.load(std::memory_order_acquire)) {
            reload_pending_.store(false, std::memory_order_release);
            std::unique_ptr<Router> new_router;
            {
                std::lock_guard<std::mutex> lock(router_mutex_);
                new_router = std::move(pending_router_);
            }
            if (new_router) {
                apply_new_router(*new_router);
            }
        }
        int n = epoll_wait(epoll_fd_, events, MAX_EVENTS, 1000);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }

        for (int i = 0; i < n; ++i) {
            if (events[i].data.fd == timer_fd_) {
                uint64_t expirations;
                if (read(timer_fd_, &expirations, sizeof(expirations)) > 0) {
                    check_unhealthy_backends();
                }
            } else {
                int sock = events[i].data.fd;
                auto it = watch_sockets_.find(sock);
                if (it != watch_sockets_.end()) {
                    auto backend = it->second;
                    backend->is_healthy.store(false);
                    epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, sock, nullptr);
                    close(sock);
                    watch_sockets_.erase(it);
                    if (g_observer) {
                        std::string sname = "unknown";
                        int healthy_count = 0;
                        for (const auto& pair : router_.get_routes()) {
                            for (const auto& b : pair.second.backends) {
                                if (b == backend) {
                                    sname = pair.second.name;
                                    for (const auto& bb : pair.second.backends) {
                                        if (bb->is_healthy.load()) healthy_count++;
                                    }
                                    break;
                                }
                            }
                            if (sname != "unknown") break;
                        }
                        std::string msg = backend->host + ":" + std::to_string(backend->port) + " " + sname + " down | " + std::to_string(healthy_count) + " healthy";
                        g_observer->record_event(obs_queue_, EventType::HEALTH_STATE_CHANGED, INVALID_FD, INVALID_FD, msg);
                    }
                }
            }
        }
    }
}

void HealthChecker::stop() {
    running_.store(false);
}

void HealthChecker::update_router(const Router& new_router) {
    std::lock_guard<std::mutex> lock(router_mutex_);
    pending_router_ = std::make_unique<Router>(new_router);
    reload_pending_.store(true, std::memory_order_release);
}

void HealthChecker::apply_new_router(const Router& new_router) {
    router_ = new_router;
    
    // Reset watch sockets
    for (auto& pair : watch_sockets_) {
        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, pair.first, nullptr);
        close(pair.first);
    }
    watch_sockets_.clear();

    for (const auto& pair : router_.get_routes()) {
        const ServiceTarget& target = pair.second;
        for (const auto& backend : target.backends) {
            fd_t sock = try_connect(backend->host, backend->port);
            if (sock != INVALID_FD) {
                backend->is_healthy.store(true);
                struct epoll_event bev{};
                bev.events = EPOLLIN | EPOLLRDHUP;
                bev.data.fd = sock;
                epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, sock, &bev);
                watch_sockets_[sock] = backend;
            } else {
                backend->is_healthy.store(false);
            }
        }
    }
}

void HealthChecker::check_unhealthy_backends() {
    for (const auto& pair : router_.get_routes()) {
        const ServiceTarget& target = pair.second;
        for (const auto& backend : target.backends) {
            if (!backend->is_healthy.load()) {
                fd_t sock = try_connect(backend->host, backend->port);
                if (sock != INVALID_FD) {
                    backend->is_healthy.store(true);
                    struct epoll_event ev{};
                    ev.events = EPOLLIN | EPOLLRDHUP;
                    ev.data.fd = sock;
                    epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, sock, &ev);
                    watch_sockets_[sock] = backend;
                    if (g_observer) {
                        int healthy_count = 0;
                        for (const auto& bb : target.backends) {
                            if (bb->is_healthy.load()) healthy_count++;
                        }
                        std::string msg = backend->host + ":" + std::to_string(backend->port) + " " + target.name + " recovered | " + std::to_string(healthy_count) + " healthy";
                        g_observer->record_event(obs_queue_, EventType::HEALTH_STATE_CHANGED, INVALID_FD, INVALID_FD, msg);
                    }
                }
            }
        }
    }
}

fd_t HealthChecker::try_connect(const std::string& host, uint16_t port) {
    fd_t sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == INVALID_FD) {
        return INVALID_FD;
    }

    struct timeval tv{};
    tv.tv_sec = 1;
    tv.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
        close(sock);
        return INVALID_FD;
    }

    if (connect(sock, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0) {
        return sock;
    }
    
    close(sock);
    return INVALID_FD;
}
