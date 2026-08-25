#pragma once

#include "common/types.h"
#include <atomic>
#include <cstdint>
#include <string>
#include <vector>
#include <memory>
#include <mutex>
#include "routing/router.h"
#include <thread>
#include <chrono>

enum class EventType {
    CLIENT_CONNECTED,
    CLIENT_DISCONNECTED,
    BACKEND_ERROR,
    HEALTH_STATE_CHANGED,
    SYSTEM_LOG,
    LOAD_BALANCER
};

struct EventRecord {
    uint64_t timestamp_ms;
    EventType type;
    fd_t client_fd;
    fd_t backend_fd;
    std::string metadata;
};

class EventQueue {
public:
    static constexpr size_t CAPACITY = 1024;

    bool push(const EventRecord& event) {
        size_t current_tail = tail_.load(std::memory_order_relaxed);
        size_t next_tail = (current_tail + 1) % CAPACITY;
        
        if (next_tail == head_.load(std::memory_order_acquire)) {
            return false;
        }
        
        buffer_[current_tail] = event;
        tail_.store(next_tail, std::memory_order_release);
        return true;
    }

    bool pop(EventRecord& event) {
        size_t current_head = head_.load(std::memory_order_relaxed);
        
        if (current_head == tail_.load(std::memory_order_acquire)) {
            return false;
        }
        
        event = buffer_[current_head];
        head_.store((current_head + 1) % CAPACITY, std::memory_order_release);
        return true;
    }

private:
    EventRecord buffer_[CAPACITY];
    std::atomic<size_t> head_{0};
    std::atomic<size_t> tail_{0};
};

struct GlobalMetrics {
    std::atomic<uint64_t> total_connections{0};
    std::atomic<uint64_t> active_connections{0};
    std::atomic<uint64_t> bytes_c2b{0};
    std::atomic<uint64_t> bytes_b2c{0};
};

extern GlobalMetrics g_metrics;

class ObservabilityWorker {
public:
    ObservabilityWorker(const std::string& socket_path);
    ~ObservabilityWorker();

    bool init();
    void run();
    void stop();

    EventQueue* register_thread();
    void record_event(EventQueue* queue, EventType type, fd_t client_fd, fd_t backend_fd, const std::string& metadata);
    
    void update_router(const Router& new_router);

private:
    std::string socket_path_;
    fd_t epoll_fd_;
    fd_t admin_fd_;
    std::atomic<bool> running_;

    std::mutex queues_mutex_;
    std::vector<std::unique_ptr<EventQueue>> thread_queues_;

    static constexpr size_t MAX_EVENTS = 2000;
    std::vector<EventRecord> event_history_;
    size_t history_head_{0};
    size_t history_count_{0};

    Router router_;
    std::mutex router_mutex_;

    void consume_events();
    void handle_admin_accept();
    void handle_admin_request(fd_t client_fd);
    std::string generate_metrics_json();
    std::string generate_events_json();
    std::string generate_topology_json();
};

extern ObservabilityWorker* g_observer;
