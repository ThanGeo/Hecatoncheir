cmake_minimum_required(VERSION 3.10)  # Match top-level version
project(Hecatoncheir VERSION 1.0.1)

# Generate configuration header
configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/include/config_pub.h.in
    ${CMAKE_CURRENT_BINARY_DIR}/config_pub.h  # Better to put in binary dir
)

# Include directories
include_directories(SYSTEM ${MPI_INCLUDE_PATH})
include_directories(${Boost_INCLUDE_DIRS})

# Add definitions
add_definitions("-DBOOST_ALLOW_DEPRECATED_HEADERS")
add_definitions("-DBOOST_BIND_GLOBAL_PLACEHOLDERS")

# Source files
set(SrcFiles
    API/Hecatoncheir.cpp
    API/containers.cpp

    src/def.cpp
    src/containers.cpp
    src/proc.cpp
    src/utils.cpp
    
    src/env/comm_def.cpp
    src/env/comm_common.cpp
    src/env/comm_host.cpp
    src/env/comm_worker.cpp
    src/env/recv.cpp
    src/env/send.cpp
    src/env/pack.cpp
    src/env/partitioning.cpp

    src/storage/utils.cpp
    src/storage/write.cpp
    src/storage/read.cpp

    src/config/setup.cpp
    src/config/parse.cpp
    src/config/configure.cpp
    src/config/statement.cpp

    src/APRIL/generate.cpp
    src/APRIL/storage.cpp
    src/APRIL/filter.cpp
    src/APRIL/join.cpp
    
    src/TwoLayer/filter.cpp
    src/TwoLayer/range_filter.cpp
    src/TwoLayer/intersection_join_filter.cpp
    src/TwoLayer/topology_join_filter.cpp

    src/UniformGrid/filter.cpp
    src/UniformGrid/range_filter.cpp
    src/UniformGrid/knn_filter.cpp
    src/UniformGrid/dj_filter.cpp

    src/refinement/topology.cpp
)

# Create library
add_library(${PROJECT_NAME} STATIC ${SrcFiles})

# Target-specific includes (better than global include_directories)
target_include_directories(${PROJECT_NAME} PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}  # For generated config header
)

# Link dependencies the traditional way
target_link_libraries(${PROJECT_NAME} PUBLIC 
    ${Boost_LIBRARIES}
    ${MPI_CXX_LIBRARIES}
)

# Make sure include directories are set
target_include_directories(${PROJECT_NAME} PUBLIC
    ${MPI_INCLUDE_PATH}
    ${Boost_INCLUDE_DIRS}
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}
)

# Worker executable
add_executable(worker worker.cpp)
target_link_libraries(worker PRIVATE ${PROJECT_NAME})

# Resources (simplified)
file(COPY ${CMAKE_SOURCE_DIR}/Hecatoncheir/resources 
     DESTINATION ${CMAKE_BINARY_DIR}/Hecatoncheir)

# Tests subdirectory
add_subdirectory(test)