// config_parser.h — Reads a gateway.yaml file and returns a GatewayConfig.
#pragma once
#include <string>
#include "config/config_types.h"

class ConfigParser {
public:
    static GatewayConfig parse(const std::string& filepath);
};
