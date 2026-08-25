#include "load_balancer.h"
#include <random>
#include <vector>

#include <string>

BackendInstance* LoadBalancer::choose_server(const std::vector<std::shared_ptr<BackendInstance>>& pool, EventQueue* obs_queue, fd_t client_fd) {
    if (g_observer && obs_queue) {
        g_observer->record_event(obs_queue, EventType::LOAD_BALANCER, client_fd, -1, "searching for healthy instances...");
    }

    std::vector<int> healthy_indices;
    for (int i = 0; i < static_cast<int>(pool.size()); ++i) {
        if (pool[i]->is_healthy.load())healthy_indices.push_back(i);
    }

    if (healthy_indices.empty())return nullptr;
    if (healthy_indices.size() == 1) {
        if (g_observer && obs_queue) {
            std::string log_msg = pool[healthy_indices[0]]->host + ":" + std::to_string(pool[healthy_indices[0]]->port) + " from " + pool[healthy_indices[0]]->host + ":" + std::to_string(pool[healthy_indices[0]]->port) + " (Number of connections: " + std::to_string(pool[healthy_indices[0]]->active_connections.load()) + ") and None based on fallback";
            g_observer->record_event(obs_queue, EventType::LOAD_BALANCER, client_fd, -1, log_msg);
        }
        return pool[healthy_indices[0]].get();
    }
    
    thread_local std::mt19937 rng(std::random_device{}());
    int size = static_cast<int>(healthy_indices.size());
    std::uniform_int_distribution<int> dist_first(0, size - 1);
    int idx1 = dist_first(rng);

    std::uniform_int_distribution<int> dist_second(0, size - 2);
    int idx2 = dist_second(rng);
    if (idx2 >= idx1)idx2 += 1;

    int a = healthy_indices[idx1];
    int b = healthy_indices[idx2];

    BackendInstance* chosen = nullptr;
    if (pool[a]->active_connections.load() <= pool[b]->active_connections.load()) chosen = pool[a].get();
    else chosen = pool[b].get();

    if (g_observer && obs_queue) {
        std::string log_msg = chosen->host + ":" + std::to_string(chosen->port) + " from " +
                              pool[a]->host + ":" + std::to_string(pool[a]->port) + " (Number of connections: " + std::to_string(pool[a]->active_connections.load()) + ") and " +
                              pool[b]->host + ":" + std::to_string(pool[b]->port) + " (Number of connections: " + std::to_string(pool[b]->active_connections.load()) + ") based on P2C";
        g_observer->record_event(obs_queue, EventType::LOAD_BALANCER, client_fd, -1, log_msg);
    }

    return chosen;
}
