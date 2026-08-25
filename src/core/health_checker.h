#pragma once
#include "routing/router.h"
#include "common/types.h"
#include <atomic>
#include <unordered_map>
#include <memory>
#include <mutex>

class EventQueue;

class HealthChecker {
public:
    HealthChecker(const Router& router, int interval_seconds);
    ~HealthChecker();
    bool init();
    void run();
    void stop();
    void update_router(const Router& new_router);

private:
    Router router_;
    int interval_seconds_;
    int epoll_fd_;
    int timer_fd_;
    std::atomic<bool> running_;
    std::atomic<bool> reload_pending_{false};
    std::mutex router_mutex_;
    std::unique_ptr<Router> pending_router_;

    std::unordered_map<int, std::shared_ptr<BackendInstance>> watch_sockets_;
    EventQueue* obs_queue_;

    void check_unhealthy_backends();
    fd_t try_connect(const std::string& host, uint16_t port);
    void apply_new_router(const Router& new_router);
};
