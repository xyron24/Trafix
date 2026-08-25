#include "data_forwarder.h"
#include "event_loop.h"
#include "socket_utils.h"
#include <fcntl.h>
#include <unistd.h>
#include <cstdio>
#include <cerrno>
#include <cstring>

namespace DataForwarder {
    bool init_pipes(Connection* conn) {
        if (pipe2(conn->pipe_c2b, O_CLOEXEC | O_NONBLOCK) < 0) {
            return false;
        }
        if (pipe2(conn->pipe_b2c, O_CLOEXEC | O_NONBLOCK) < 0) {
            close_fd(conn->pipe_c2b[0]);
            close_fd(conn->pipe_c2b[1]);
            return false;
        }
        conn->c2b_pipe_bytes = 0;
        conn->b2c_pipe_bytes = 0;
        return true;
    }

    void close_pipes(Connection* conn) {
        close_fd(conn->pipe_c2b[0]);
        close_fd(conn->pipe_c2b[1]);
        close_fd(conn->pipe_b2c[0]);
        close_fd(conn->pipe_b2c[1]);
    }
}
