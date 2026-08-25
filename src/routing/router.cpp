// router.cpp — Implementation of the Router route table and BackendInstance.
#include "router.h"

BackendInstance::BackendInstance(const std::string& h, uint16_t p)
    : host(h), port(p), active_connections(0), is_healthy(true) {}

Router Router::from_config(const GatewayConfig& config) {
    Router router;
    for (const auto& svc : config.services) {
        ServiceTarget target;
        target.name = svc.name;
        for (const BackendTarget& bt : svc.backends) {
            target.backends.push_back(std::make_shared<BackendInstance>(bt.host, bt.port));
        }
        router.add_route(svc.listen_port, std::move(target));
    }
    return router;
}



void Router::add_route(uint16_t listen_port, ServiceTarget target) {
    // Inserting with [] overwrites any previously registered route for this port (last write wins).
    routes_[listen_port] = std::move(target);
}

const ServiceTarget* Router::resolve(uint16_t listen_port) const {
    auto it = routes_.find(listen_port);
    if (it == routes_.end()) {
        return nullptr;  // No route registered for this port
    }
    return &it->second;
}

const std::unordered_map<uint16_t, ServiceTarget>& Router::get_routes() const {
    return routes_;
}
