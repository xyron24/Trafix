// socket_utils.cpp - TCP socket helper implementations
#include "socket_utils.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <iostream>

fd_t create_listener(uint16_t port) {
  fd_t sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock == INVALID_FD) {
    return INVALID_FD;
  }

  int opt = 1;
  if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
    close(sock);
    return INVALID_FD;
  }

  int reuseport = 1;
  if (setsockopt(sock, SOL_SOCKET, SO_REUSEPORT, &reuseport, sizeof(reuseport)) < 0) {
      close(sock);
      return INVALID_FD;
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = htons(port);

  if (bind(sock, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
    close(sock);
    return INVALID_FD;
  }

  if (listen(sock, 128) < 0) {
    close(sock);
    return INVALID_FD;
  }

  int qlen = 5;
  if (setsockopt(sock, IPPROTO_TCP, TCP_FASTOPEN, &qlen, sizeof(qlen)) < 0) {
      // Ignored if failed
  }

  return sock;
}

fd_t accept_client(fd_t listener_fd, std::string &client_ip_out) {
  sockaddr_in client_addr{};
  socklen_t addr_len = sizeof(client_addr);

  fd_t client_fd = accept(
      listener_fd, reinterpret_cast<sockaddr *>(&client_addr), &addr_len);
  if (client_fd < 0) {
    return INVALID_FD;
  }

  char ip_buf[INET_ADDRSTRLEN];
  if (inet_ntop(AF_INET, &client_addr.sin_addr, ip_buf, sizeof(ip_buf)) ==
      nullptr) {
    client_ip_out = "unknown";
  } else {
    client_ip_out = ip_buf;
  }

  return client_fd;
}

bool set_nonblocking(fd_t fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0) {
    return false;
  }

  if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
    return false;
  }

  return true;
}

void close_fd(fd_t fd) {
  if (fd == INVALID_FD) {
    return;
  }

  while (close(fd) < 0) {
    if (errno != EINTR) {
      break;
    }
  }
}

fd_t connect_to_backend(const std::string &host, uint16_t port) {
  fd_t sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock == INVALID_FD) {
    return INVALID_FD;
  }
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
    close(sock);
    return INVALID_FD;
  }
  if (connect(sock, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
    close(sock);
    return INVALID_FD;
  }
  return sock;
}
