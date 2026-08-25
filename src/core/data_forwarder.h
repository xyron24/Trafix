#pragma once
#include "common/types.h"
struct Connection;
namespace DataForwarder {
    bool init_pipes(Connection* conn);
    void close_pipes(Connection* conn);
}