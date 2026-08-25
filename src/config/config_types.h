// config_types.h — Data structs that represent the parsed gateway configuration.
#pragma once
#include <string>
#include <vector>
#include <cstdint>

struct BackendTarget {
    std::string host;
    uint16_t port;
};

struct ServiceConfig {
    std::string name;
    uint16_t listen_port;
    std::vector<BackendTarget> backends;  // changed from single BackendTarget to vector
};

struct GatewayConfig {
    std::vector<ServiceConfig> services;
};
