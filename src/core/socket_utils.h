// socket_utils.h - TCP socket helper declarations
#pragma once

#include <cstdint>
#include <string>

#include "common/types.h"

// creates socket listen on port with SO_REUSEADDR set , returns listners fd or -1.
fd_t create_listener(uint16_t port);

// accepts connection on listener_fd ,writes client's ipv4 addr into out returns the fd or -1 .
fd_t accept_client(fd_t listener_fd, std::string& client_ip_out);

// Sets fd to non-blocking mode using fcntl O_NONBLOCK , boolean .
bool set_nonblocking(fd_t fd);

// closes fd, retrying automatically on EINTR.
void close_fd(fd_t fd);

fd_t connect_to_backend(const std::string& host, uint16_t port);
