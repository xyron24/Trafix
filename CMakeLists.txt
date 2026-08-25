cmake_minimum_required(VERSION 3.16)
project(NetworkGateway LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(FetchContent)

set(CMAKE_POLICY_VERSION_MINIMUM 3.5)
FetchContent_Declare(
  yaml-cpp
  GIT_REPOSITORY https://github.com/jbeder/yaml-cpp.git
  GIT_TAG master
)
set(YAML_CPP_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(YAML_CPP_BUILD_TOOLS OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(yaml-cpp)

find_package(Threads REQUIRED)

add_executable(gateway
    src/main.cpp
    src/core/socket_utils.cpp
    src/core/event_loop.cpp
    src/core/data_forwarder.cpp
    src/core/health_checker.cpp
    src/config/config_parser.cpp
    src/routing/router.cpp
    src/routing/load_balancer.cpp
    src/observability/observer.cpp
)

target_include_directories(gateway PRIVATE include src)
target_compile_options(gateway PRIVATE -Wall -Wextra)
target_link_libraries(gateway PRIVATE yaml-cpp::yaml-cpp Threads::Threads)
