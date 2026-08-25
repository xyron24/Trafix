// main.cpp - gateway entry point: builds a Router and runs the epoll event loop
#include <cstdio>
#include <iostream>
#include <csignal>
#include <fstream>
#include <unistd.h>

#include "core/event_loop.h"
#include "routing/router.h"
#include "routing/load_balancer.h"
#include "config/config_parser.h"
#include <thread>
#include <vector>
#include <atomic>
#include <memory>
#include "core/health_checker.h"
#include "observability/observer.h"
static std::vector<EventLoop*> g_loops;
static std::atomic<bool> g_running{true};
static HealthChecker* g_health_checker = nullptr;
static EventQueue* g_main_queue = nullptr;
static std::atomic<bool> g_reload_pending{false};
void signal_handler(int sig) {
    if (sig == SIGHUP) {
        g_reload_pending.store(true);
    } else {
        g_running.store(false);
        if (g_health_checker) {
            g_health_checker->stop();
        }
        if (g_observer) {
            g_observer->stop();
        }
        for (auto* loop : g_loops) {
            if (loop) loop->shutdown();
        }
    }
}
int main(int argc, char** argv) {
    //close any already running gateway process when running . This was causing a lot of hidden issues during testing. 
    const std::string pid_file_path = "/tmp/gateway.pid";
    
    std::ifstream pid_file_in(pid_file_path);
    if (pid_file_in.is_open()) {
        pid_t old_pid;
        if (pid_file_in >> old_pid) {
            if (kill(old_pid, 0) == 0) {
                std::printf("Killing existing gateway process (PID %d)...\n", old_pid);
                kill(old_pid, SIGTERM);
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                if (kill(old_pid, 0) == 0) {
                    kill(old_pid, SIGKILL);
                }
            }
        }
        pid_file_in.close();
    }

    std::ofstream pid_file_out(pid_file_path);
    if (pid_file_out.is_open()) {
        pid_file_out << getpid() << "\n";
        pid_file_out.close();
    }

    std::string config_path = "config/gateway.yaml";
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == "--config" && i + 1 < argc) {
            config_path = argv[++i];
        }
    }
    
    std::unique_ptr<ObservabilityWorker> observer = std::make_unique<ObservabilityWorker>("/tmp/gateway_admin.sock");
    std::thread obs_thread;
    if (observer->init()) {
        g_observer = observer.get();
        obs_thread = std::thread([&observer]() {
            observer->run();
        });
        g_main_queue = g_observer->register_thread();
    }
    
    GatewayConfig config;
    try {
        config = ConfigParser::parse(config_path);
    } catch (const std::exception& e) {
        if (g_observer) {
            g_observer->record_event(g_main_queue, EventType::SYSTEM_LOG, INVALID_FD, INVALID_FD, std::string("Error parsing config: ") + e.what());
        }
        return 1;
    }
    Router router = Router::from_config(config);
    if (g_observer) {
        g_observer->update_router(router);
    }
    LoadBalancer load_balancer;
    
    HealthChecker health_checker(router, 5);
    if (!health_checker.init()) {
        if (g_observer) {
            g_observer->record_event(g_main_queue, EventType::SYSTEM_LOG, INVALID_FD, INVALID_FD, "failed to initialise health checker");
        }
        return 1;
    }
    g_health_checker = &health_checker;
    std::thread health_thread([&health_checker]() {
        health_checker.run();
    });
    
    struct sigaction sa;
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
    sigaction(SIGHUP, &sa, nullptr);
    int num_workers = std::thread::hardware_concurrency();
    if (num_workers == 0) num_workers = 1;
    std::vector<std::unique_ptr<EventLoop>> loops;
    std::vector<std::thread> threads;
    for (int i = 0; i < num_workers; ++i) {
        auto loop = std::make_unique<EventLoop>();
        if (!loop->init(config_path, router, load_balancer)) {
            if (g_observer) {
                g_observer->record_event(g_main_queue, EventType::SYSTEM_LOG, INVALID_FD, INVALID_FD, "failed to initialise event loop");
            }
            return 1;
        }
        g_loops.push_back(loop.get());
        threads.emplace_back([l = loop.get()]() {
            l->run();
        });
        loops.push_back(std::move(loop));
    }
    
    if (g_observer) {
        int total_servers = 0;
        int healthy_servers = 0;
        for (const auto& entry : router.get_routes()) {
            const ServiceTarget& target = entry.second;
            std::string log_msg = "Listening on -> ";
            if (!target.backends.empty()) {
                log_msg += target.backends[0]->host + ":" + std::to_string(target.backends[0]->port);
            } else {
                log_msg += "no backends";
            }
            log_msg += " (" + target.name + ") {" + std::to_string(num_workers) + " threads}";
            g_observer->record_event(g_main_queue, EventType::SYSTEM_LOG, INVALID_FD, INVALID_FD, log_msg);

            for (const auto& backend : target.backends) {
                total_servers++;
                if (backend->is_healthy.load()) healthy_servers++;
            }
        }
        std::string stats_msg = "Gateway Started | " + std::to_string(total_servers) + " servers | " + std::to_string(healthy_servers) + " healthy | 0 connected";
        g_observer->record_event(g_main_queue, EventType::SYSTEM_LOG, INVALID_FD, INVALID_FD, stats_msg);
    }

    while (g_running.load()) {
        if (g_reload_pending.exchange(false)) {
            try {
                GatewayConfig config = ConfigParser::parse(config_path);
                Router new_router = Router::from_config(config);
                for (auto* loop : g_loops) {
                    if (loop) loop->push_new_router(new_router);
                }
                if (g_health_checker) {
                    g_health_checker->update_router(new_router);
                }
                if (g_observer) {
                    g_observer->update_router(new_router);
                    g_observer->record_event(g_main_queue, EventType::SYSTEM_LOG, INVALID_FD, INVALID_FD, "gateway configuration dynamically reloaded");

                    int total_servers = 0;
                    int healthy_servers = 0;
                    for (const auto& entry : new_router.get_routes()) {
                        const ServiceTarget& target = entry.second;
                        std::string log_msg = "Listening on -> ";
                        if (!target.backends.empty()) {
                            log_msg += target.backends[0]->host + ":" + std::to_string(target.backends[0]->port);
                        } else {
                            log_msg += "no backends";
                        }
                        log_msg += " (" + target.name + ") {" + std::to_string(num_workers) + " threads}";
                        g_observer->record_event(g_main_queue, EventType::SYSTEM_LOG, INVALID_FD, INVALID_FD, log_msg);

                        for (const auto& backend : target.backends) {
                            total_servers++;
                            if (backend->is_healthy.load()) healthy_servers++;
                        }
                    }
                    std::string stats_msg = "Config Reloaded | " + std::to_string(total_servers) + " servers | " + std::to_string(healthy_servers) + " healthy | 0 connected";
                    g_observer->record_event(g_main_queue, EventType::SYSTEM_LOG, INVALID_FD, INVALID_FD, stats_msg);
                }
            } catch (const std::exception& e) {
                if (g_observer) {
                    g_observer->record_event(g_main_queue, EventType::SYSTEM_LOG, INVALID_FD, INVALID_FD, std::string("gateway config reload failed: ") + e.what());
                }
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }
    
    if (g_health_checker) {
        g_health_checker->stop();
    }
    if (health_thread.joinable()) {
        health_thread.join();
    }
    
    if (g_observer) {
        g_observer->record_event(g_main_queue, EventType::SYSTEM_LOG, INVALID_FD, INVALID_FD, "gateway shut down.");
        // Give observer thread a tiny moment to flush events
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    if (obs_thread.joinable()) {
        obs_thread.join();
    }
    
    std::remove(pid_file_path.c_str());
    return 0;
}
