// event_loop.cpp - epoll event loop: one listener per route, routes clients to the correct backend
#include "event_loop.h"
#include "socket_utils.h"
#include "data_forwarder.h"
#include "config/config_parser.h"
#include "observability/observer.h"
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <chrono>
#include <sstream>

static constexpr int MAX_EVENTS = 64;

namespace {
void notify_client_disconnected(fd_t client_fd) {
    static constexpr char kDisconnectMsg[] = "DISCONNECTED\n";
    size_t offset = 0;
    while (offset < sizeof(kDisconnectMsg) - 1) {
        ssize_t sent = ::send(client_fd, kDisconnectMsg + offset, (sizeof(kDisconnectMsg) - 1) - offset, MSG_NOSIGNAL | MSG_DONTWAIT);
        if (sent > 0) {
            offset += static_cast<size_t>(sent);
            continue;
        }
        if (sent < 0 && errno == EINTR) {
            continue;
        }
        break;
    }
}
}

EventLoop::EventLoop()
    : epoll_fd_(INVALID_FD), running_(false), reload_pending_(false), obs_queue_(nullptr) {}

EventLoop::~EventLoop() {
    for (auto& pair : connections_) {
        close_fd(pair.first);
    }
    connections_.clear();
    for (auto& pair : listeners_) {
        close_fd(pair.first);
    }
    listeners_.clear();
    if (epoll_fd_ != INVALID_FD) {
        close_fd(epoll_fd_);
    }
}

bool EventLoop::init(const std::string& config_path, const Router& router, LoadBalancer& lb) {
    config_path_ = config_path;
    router_ = router;
    load_balancer_ = &lb;

    if (g_observer) {
        obs_queue_ = g_observer->register_thread();
    } else {
        obs_queue_ = nullptr;
    }

    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ == INVALID_FD) {
        if (g_observer && obs_queue_) {
            g_observer->record_event(obs_queue_, EventType::SYSTEM_LOG, INVALID_FD, INVALID_FD, std::string("event_loop: epoll_create1 failed: ") + std::strerror(errno));
        }
        return false;
    }

    for (const auto& entry : router.get_routes()) {
        uint16_t port = entry.first;

        fd_t listener_fd = create_listener(port);
        if (listener_fd == INVALID_FD) {
            if (g_observer && obs_queue_) {
                g_observer->record_event(obs_queue_, EventType::SYSTEM_LOG, INVALID_FD, INVALID_FD, "event_loop: failed to create listener on port " + std::to_string(port));
            }
            return false;
        }
        if (!set_nonblocking(listener_fd)) {
            if (g_observer && obs_queue_) {
                g_observer->record_event(obs_queue_, EventType::SYSTEM_LOG, INVALID_FD, INVALID_FD, "event_loop: failed to set listener non-blocking on port " + std::to_string(port));
            }
            close_fd(listener_fd);
            return false;
        }
        struct epoll_event ev{};
        ev.events  = EPOLLIN;
        ev.data.fd = listener_fd;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listener_fd, &ev) < 0) {
            if (g_observer && obs_queue_) {
                g_observer->record_event(obs_queue_, EventType::SYSTEM_LOG, INVALID_FD, INVALID_FD, "event_loop: epoll_ctl add listener failed for port " + std::to_string(port) + ": " + std::strerror(errno));
            }
            close_fd(listener_fd);
            return false;
        }
        listeners_[listener_fd] = port;
    }
    return true;
}

void EventLoop::run() {
    running_ = true;
    struct epoll_event events[MAX_EVENTS];
    while (running_) {
        if (reload_pending_.load(std::memory_order_acquire)) {
            reload_pending_.store(false, std::memory_order_release);
            std::unique_ptr<Router> new_router;
            {
                std::lock_guard<std::mutex> lock(pending_router_mutex_);
                new_router = std::move(pending_router_);
            }
            if (new_router) {
                apply_new_router(*new_router);
            }
        }
        int n = epoll_wait(epoll_fd_, events, MAX_EVENTS, 100);
        if (n == 0) continue;
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (g_observer && obs_queue_) {
                g_observer->record_event(obs_queue_, EventType::SYSTEM_LOG, INVALID_FD, INVALID_FD, std::string("event_loop: epoll_wait failed: ") + std::strerror(errno));
            }
            break;
        }
        for (int i = 0; i < n; ++i) {
            fd_t fd = events[i].data.fd;
            uint32_t ev = events[i].events;
            if (listeners_.count(fd)) {
                handle_accept(fd);
                continue;
            }
            if (ev & EPOLLIN) {
                handle_read(fd);
            }
            if (connections_.count(fd) == 0) {
                continue;
            }
            if (ev & EPOLLOUT) {
                handle_write(fd);
            }
            if (connections_.count(fd) == 0) {
                continue;
            }
            if (ev & (EPOLLERR | EPOLLHUP)) {
                if ((ev & EPOLLHUP) && (ev & EPOLLIN) && !(ev & EPOLLERR)) {
                    // Graceful EOF with pending data. Let handle_read handle it in a future iteration.
                    continue;
                }
                handle_disconnect(fd);
            }
        }
    }
}

void EventLoop::shutdown() {
    running_ = false;
}

void EventLoop::push_new_router(const Router& new_router) {
    std::lock_guard<std::mutex> lock(pending_router_mutex_);
    pending_router_ = std::make_unique<Router>(new_router);
    reload_pending_.store(true, std::memory_order_release);
}

void EventLoop::apply_new_router(const Router& new_router) {
    try {
        std::unordered_map<uint16_t, fd_t> current_ports;
        for (const auto& pair : listeners_) {
            current_ports[pair.second] = pair.first;
        }

        // Add new listeners
        for (const auto& entry : new_router.get_routes()) {
            uint16_t port = entry.first;
            if (current_ports.count(port) == 0) {
                fd_t listener_fd = create_listener(port);
                if (listener_fd != INVALID_FD) {
                    set_nonblocking(listener_fd);
                    struct epoll_event ev{};
                    ev.events = EPOLLIN;
                    ev.data.fd = listener_fd;
                    epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listener_fd, &ev);
                    listeners_[listener_fd] = port;
                }
            }
        }

        // Remove old listeners
        for (const auto& pair : current_ports) {
            uint16_t port = pair.first;
            if (new_router.resolve(port) == nullptr) {
                fd_t fd = pair.second;
                epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
                close_fd(fd);
                listeners_.erase(fd);
            }
        }
        router_ = new_router;
    } catch (const std::exception& e) {
        if (g_observer && obs_queue_) {
            g_observer->record_event(obs_queue_, EventType::SYSTEM_LOG, INVALID_FD, INVALID_FD, std::string("gateway config reload failed: ") + e.what());
        }
    }
}

void EventLoop::handle_accept(fd_t listener_fd) {
    auto lit = listeners_.find(listener_fd);
    if (lit == listeners_.end()) {
        return;
    }
    uint16_t listen_port = lit->second;
    const ServiceTarget* target = router_.resolve(listen_port);
    if (target == nullptr) {
        if (g_observer && obs_queue_) {
            g_observer->record_event(obs_queue_, EventType::SYSTEM_LOG, INVALID_FD, INVALID_FD, "event_loop: no route for port " + std::to_string(listen_port));
        }
        return;
    }
    while (true) {
        std::string client_ip;
        fd_t client_fd = accept_client(listener_fd, client_ip);
        if (client_fd == INVALID_FD) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            if (g_observer && obs_queue_) {
                g_observer->record_event(obs_queue_, EventType::SYSTEM_LOG, INVALID_FD, INVALID_FD, std::string("event_loop: accept_client failed: ") + std::strerror(errno));
            }
            break;
        }
        int max_retries = static_cast<int>(target->backends.size());
        BackendInstance* chosen = nullptr;
        fd_t backend_fd = INVALID_FD;
        for (int attempt = 0; attempt < max_retries; ++attempt) {
            chosen = load_balancer_->choose_server(target->backends, obs_queue_, client_fd);
            if (chosen == nullptr) {
                break;
            }
            chosen->active_connections.fetch_add(1);
            backend_fd = connect_to_backend(chosen->host, chosen->port);
            if (backend_fd != INVALID_FD) {
                break;
            }
            chosen->is_healthy.store(false, std::memory_order_release);
            chosen->active_connections.fetch_sub(1);
            if (g_observer && obs_queue_) {
                g_observer->record_event(obs_queue_, EventType::BACKEND_ERROR, client_fd, INVALID_FD, "[FAILOVER] " + chosen->host + ":" + std::to_string(chosen->port) + " (" + target->name + ") connect failed, marked unhealthy | client fd " + std::to_string(client_fd) + " retry " + std::to_string(attempt + 1) + "/" + std::to_string(max_retries) + ", load balancer selecting another server");
            }
            chosen = nullptr;
        }
        if (chosen == nullptr || backend_fd == INVALID_FD) {
            if (g_observer && obs_queue_) {
                g_observer->record_event(obs_queue_, EventType::BACKEND_ERROR, client_fd, INVALID_FD, "[FAILOVER] " + target->name + " all " + std::to_string(max_retries) + " backends exhausted for port " + std::to_string(listen_port) + ", client fd " + std::to_string(client_fd) + " dropped");
                g_observer->record_event(obs_queue_, EventType::CLIENT_DISCONNECTED, client_fd, INVALID_FD, client_ip + " (" + target->name + ") | no healthy backend available");
            }
            close_fd(client_fd);
            continue;
        }
        if (!set_nonblocking(client_fd) || !set_nonblocking(backend_fd)) {
            if (g_observer && obs_queue_) {
                g_observer->record_event(obs_queue_, EventType::CLIENT_DISCONNECTED, client_fd, backend_fd, client_ip + " (" + target->name + ") | failed to set fds non-blocking");
            }
            chosen->active_connections.fetch_sub(1);
            close_fd(client_fd);
            close_fd(backend_fd);
            continue;
        }
        auto conn = std::make_shared<Connection>();
        conn->client_fd  = client_fd;
        conn->backend_fd = backend_fd;
        conn->backend_instance = chosen;
        conn->client_ip = client_ip;
        conn->service_name = target->name;

        if (!DataForwarder::init_pipes(conn.get())) {
            if (g_observer && obs_queue_) {
                g_observer->record_event(obs_queue_, EventType::CLIENT_DISCONNECTED, client_fd, backend_fd, client_ip + " (" + target->name + ") | pipe initialization failed");
            }
            chosen->active_connections.fetch_sub(1);
            close_fd(client_fd);
            close_fd(backend_fd);
            continue;
        }
        struct epoll_event ev{};
        ev.events  = EPOLLIN;
        ev.data.fd = client_fd;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client_fd, &ev) < 0) {
            if (g_observer && obs_queue_) {
                g_observer->record_event(obs_queue_, EventType::CLIENT_DISCONNECTED, client_fd, backend_fd, client_ip + " (" + target->name + ") | epoll_ctl add client_fd failed");
            }
            chosen->active_connections.fetch_sub(1);
            close_fd(client_fd);
            close_fd(backend_fd);
            continue;
        }

        ev.data.fd = backend_fd;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, backend_fd, &ev) < 0) {
            if (g_observer && obs_queue_) {
                g_observer->record_event(obs_queue_, EventType::CLIENT_DISCONNECTED, client_fd, backend_fd, client_ip + " (" + target->name + ") | epoll_ctl add backend_fd failed");
            }
            chosen->active_connections.fetch_sub(1);
            epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, client_fd, nullptr);
            close_fd(client_fd);
            close_fd(backend_fd);
            continue;
        }
        connections_[client_fd]  = conn;
        connections_[backend_fd] = conn;
        
        g_metrics.total_connections.fetch_add(1, std::memory_order_relaxed);
        g_metrics.active_connections.fetch_add(1, std::memory_order_relaxed);
        if (g_observer) {
            std::string log_msg = chosen->host + ":" + std::to_string(chosen->port) + " (" + target->name + ") | " + std::to_string(backend_fd) + " <-> " + std::to_string(client_fd) + " | Active Connections: " + std::to_string(g_metrics.active_connections.load());
            g_observer->record_event(obs_queue_, EventType::CLIENT_CONNECTED, client_fd, backend_fd, log_msg);
        }
    }
}

void EventLoop::handle_read(fd_t fd) {
    auto it = connections_.find(fd);
    if (it == connections_.end()) {
        return;
    }
    auto conn = it->second;

    fd_t peer_fd;
    fd_t pipe_write;
    fd_t pipe_read;
    size_t* pipe_bytes;

    if (fd == conn->client_fd) {
        peer_fd   = conn->backend_fd;
        pipe_write = conn->pipe_c2b[1];
        pipe_read = conn->pipe_c2b[0];
        pipe_bytes = &conn->c2b_pipe_bytes;
    } else {
        peer_fd   = conn->client_fd;
        pipe_write = conn->pipe_b2c[1];
        pipe_read = conn->pipe_b2c[0];
        pipe_bytes = &conn->b2c_pipe_bytes;
    }

    auto start_time = std::chrono::high_resolution_clock::now();
    ssize_t bytes = splice(fd, nullptr, pipe_write, nullptr, BUFFER_SIZE, SPLICE_F_MOVE | SPLICE_F_NONBLOCK);
    auto end_time = std::chrono::high_resolution_clock::now();
    if (bytes < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return;
        }
        if (fd == conn->backend_fd) {
            notify_client_disconnected(conn->client_fd);
        }
        remove_connection(fd);
        return;
    }
    if (bytes == 0) {
        if (fd == conn->client_fd) {
            while (*pipe_bytes > 0) {
                ssize_t w = splice(pipe_read, nullptr, peer_fd, nullptr, *pipe_bytes, SPLICE_F_MOVE | SPLICE_F_NONBLOCK);
                if (w <= 0) break;
                *pipe_bytes -= w;
            }
            ::shutdown(peer_fd, SHUT_WR);
            epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
        } else {
            while (*pipe_bytes > 0) {
                ssize_t w = splice(pipe_read, nullptr, peer_fd, nullptr, *pipe_bytes, SPLICE_F_MOVE | SPLICE_F_NONBLOCK);
                if (w <= 0) break;
                *pipe_bytes -= w;
            }
            notify_client_disconnected(conn->client_fd);
            remove_connection(fd);
        }
        return;
    }
    *pipe_bytes += bytes;
    if (fd == conn->client_fd) {
        g_metrics.bytes_c2b.fetch_add(bytes, std::memory_order_relaxed);
        if (g_observer) {
            std::ostringstream ss;
            ss << std::this_thread::get_id();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
            std::string log_msg = "[REQUEST] | " + conn->backend_instance->host + ":" + std::to_string(conn->backend_instance->port) + " (" + conn->service_name + ") | Thread " + ss.str() + " | " + std::to_string(bytes) + " bytes | latency: " + std::to_string(duration) + " us (TCP splice)";
            g_observer->record_event(obs_queue_, EventType::SYSTEM_LOG, conn->client_fd, conn->backend_fd, log_msg);
        }
    } else {
        g_metrics.bytes_b2c.fetch_add(bytes, std::memory_order_relaxed);
    }

    // immediately try to forward to peer
    ssize_t w = splice(pipe_read, nullptr, peer_fd, nullptr, *pipe_bytes, SPLICE_F_MOVE | SPLICE_F_NONBLOCK);
    if (w > 0) {
        *pipe_bytes -= w;
    } else if (w < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
        if (fd == conn->backend_fd) {
            notify_client_disconnected(conn->client_fd);
        }
        remove_connection(fd);
        return;
    }
    if (*pipe_bytes > 0) {
        struct epoll_event ev{};
        ev.events  = EPOLLIN | EPOLLOUT;
        ev.data.fd = peer_fd;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, peer_fd, &ev) < 0) {
            if (errno == ENOENT) {
                ev.events = EPOLLOUT;
                epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, peer_fd, &ev);
            }
        }
    }
}

void EventLoop::handle_write(fd_t fd) {
    auto it = connections_.find(fd);
    if (it == connections_.end()) {
        return;
    }
    auto conn = it->second;

    fd_t pipe_read;
    size_t* pipe_bytes;

    if (fd == conn->client_fd) {
        pipe_read = conn->pipe_b2c[0];
        pipe_bytes = &conn->b2c_pipe_bytes;
    } else {
        pipe_read = conn->pipe_c2b[0];
        pipe_bytes = &conn->c2b_pipe_bytes;
    }

    if (*pipe_bytes == 0) {
        struct epoll_event ev{};
        ev.events  = EPOLLIN;
        ev.data.fd = fd;
        epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev);
        return;
    }

    ssize_t written = splice(pipe_read, nullptr, fd, nullptr, *pipe_bytes, SPLICE_F_MOVE | SPLICE_F_NONBLOCK);
    if (written < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return;
        }
        if (fd == conn->backend_fd) {
            notify_client_disconnected(conn->client_fd);
        }
        remove_connection(fd);
        return;
    }
    
    *pipe_bytes -= written;
    if (*pipe_bytes == 0) {
        struct epoll_event ev{};
        ev.events  = EPOLLIN;
        ev.data.fd = fd;
        epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev);
    }
}

void EventLoop::handle_disconnect(fd_t fd) {
    auto it = connections_.find(fd);
    if (it != connections_.end()) {
        auto conn = it->second;
        if (conn->backend_fd == fd && conn->backend_instance) {
            conn->backend_instance->is_healthy.store(false, std::memory_order_release);
            if (g_observer) {
                g_observer->record_event(obs_queue_, EventType::BACKEND_ERROR, conn->client_fd, conn->backend_fd, conn->backend_instance->host);
            }
            notify_client_disconnected(conn->client_fd);
        }
    }
    remove_connection(fd);
}

void EventLoop::remove_connection(fd_t fd) {
    auto it = connections_.find(fd);
    if (it == connections_.end()) {
        return;
    }
    auto conn = it->second;
    
    if (conn->backend_instance) {
        conn->backend_instance->active_connections.fetch_sub(1);
    }
    
    epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, conn->client_fd, nullptr);
    epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, conn->backend_fd, nullptr);
    close_fd(conn->client_fd);
    close_fd(conn->backend_fd);
    DataForwarder::close_pipes(conn.get());
    connections_.erase(conn->client_fd);
    connections_.erase(conn->backend_fd);
    
    g_metrics.active_connections.fetch_sub(1, std::memory_order_relaxed);
    if (g_observer) {
        std::string log_msg = conn->backend_instance->host + ":" + std::to_string(conn->backend_instance->port) + " (" + conn->service_name + ") | Active Connections: " + std::to_string(g_metrics.active_connections.load());
        g_observer->record_event(obs_queue_, EventType::CLIENT_DISCONNECTED, conn->client_fd, conn->backend_fd, log_msg);
    }
}
