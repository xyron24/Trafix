#pragma once
// router.h — Route table mapping listen ports to backend service pools.
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>
#include <memory>
#include <atomic>
#include "config/config_types.h"

struct BackendInstance {
    std::string host;
    uint16_t port;
    std::atomic<int> active_connections;
    std::atomic<bool> is_healthy;

    BackendInstance(const std::string& h, uint16_t p);
};

struct ServiceTarget {
    std::string name;
    std::vector<std::shared_ptr<BackendInstance>> backends;
};


class Router {
public:
    // Build a Router from a parsed GatewayConfig — one route per service entry.
    static Router from_config(const GatewayConfig& config);

    // Add a route 
    void add_route(uint16_t listen_port, ServiceTarget target);

    // Resolve a listen port to its backend target.
    const ServiceTarget* resolve(uint16_t listen_port) const;

    // Returns the full route table (used by the event loop)
    const std::unordered_map<uint16_t, ServiceTarget>& get_routes() const;

private:
    std::unordered_map<uint16_t, ServiceTarget> routes_;
};
