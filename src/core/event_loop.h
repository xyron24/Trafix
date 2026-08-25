// event_loop.h - epoll-based event loop with multi-port routing via Router
#pragma once
#include "common/types.h"
#include "routing/router.h"
#include "routing/load_balancer.h"
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <atomic>
#include <mutex>

class EventQueue;

struct Connection {
    fd_t client_fd;
    fd_t backend_fd;
    fd_t pipe_c2b[2];
    fd_t pipe_b2c[2];
    size_t c2b_pipe_bytes;
    size_t b2c_pipe_bytes;
    BackendInstance* backend_instance;  // tracks which backend this connection uses
    std::string client_ip;
    std::string service_name;
};

class EventLoop {
public:
    EventLoop();
    ~EventLoop();
    bool init(const std::string& config_path, const Router& router, LoadBalancer& lb);
    void run();
    void shutdown();
    void push_new_router(const Router& new_router);

private:
    void apply_new_router(const Router& new_router);
    fd_t epoll_fd_;
    std::atomic<bool> running_;
    std::atomic<bool> reload_pending_;
    std::mutex pending_router_mutex_;
    std::unique_ptr<Router> pending_router_;
    std::string config_path_;
    std::unordered_map<fd_t, uint16_t> listeners_;
    Router router_;
    LoadBalancer* load_balancer_;
    EventQueue* obs_queue_;
    std::unordered_map<fd_t, std::shared_ptr<Connection>> connections_;

    void handle_accept(fd_t listener_fd);
    void handle_read(fd_t fd);
    void handle_write(fd_t fd);
    void handle_disconnect(fd_t fd);
    void remove_connection(fd_t fd);
};
