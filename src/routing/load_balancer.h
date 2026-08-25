#pragma once
#include "router.h"
#include <memory>
#include <vector>

#include "observability/observer.h"

class LoadBalancer {
public:
    BackendInstance* choose_server(const std::vector<std::shared_ptr<BackendInstance>>& pool, EventQueue* obs_queue = nullptr, fd_t client_fd = -1);
};
