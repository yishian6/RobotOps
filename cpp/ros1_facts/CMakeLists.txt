cmake_minimum_required(VERSION 3.16)
project(robotops_ros1_facts LANGUAGES CXX)

find_package(catkin REQUIRED COMPONENTS roscpp topic_tools)
find_package(Boost REQUIRED COMPONENTS system)
find_package(SQLite3 REQUIRED)
find_package(nlohmann_json REQUIRED)

add_executable(robotops_ros1_facts main.cpp)
target_compile_features(robotops_ros1_facts PRIVATE cxx_std_17)
target_compile_options(robotops_ros1_facts PRIVATE -Wall -Wextra -Wpedantic)
target_include_directories(robotops_ros1_facts PRIVATE ${catkin_INCLUDE_DIRS})
target_link_libraries(robotops_ros1_facts PRIVATE
    ${catkin_LIBRARIES} Boost::system SQLite::SQLite3 nlohmann_json::nlohmann_json
)
