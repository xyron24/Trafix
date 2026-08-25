// config_parser.cpp — Parses gateway.yaml using yaml-cpp into GatewayConfig.
#include "config/config_parser.h"
#include <yaml-cpp/yaml.h>
#include <stdexcept>
#include <string>

GatewayConfig ConfigParser::parse(const std::string& filepath) {
    YAML::Node root;
    try {
        root = YAML::LoadFile(filepath);
    } catch (const YAML::BadFile&) {
        throw std::runtime_error("config file not found: " + filepath);
    } catch (const YAML::ParserException& e) {
        throw std::runtime_error(std::string("malformed YAML: ") + e.what());
    }

    GatewayConfig config;

    // Navigate: gateway -> services
    if (!root["gateway"] || !root["gateway"]["services"]) {
        return config;
    }

    const YAML::Node& services = root["gateway"]["services"];
    for (const auto& svc : services) {
        ServiceConfig sc;
        sc.name        = svc["name"].as<std::string>();
        sc.listen_port = svc["listen_port"].as<uint16_t>();

        const YAML::Node& backends = svc["backends"];
        for (const auto& b : backends) {
            BackendTarget bt;
            bt.host = b["host"].as<std::string>();
            bt.port = b["port"].as<uint16_t>();
            sc.backends.push_back(bt);
        }

        config.services.push_back(sc);
    }

    return config;
}
