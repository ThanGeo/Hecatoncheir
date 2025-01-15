cmake_minimum_required(VERSION 3.0.0)
project(Hecatoncheir VERSION 1.0.0)

set(CMAKE_CXX_FLAGS "-fopenmp")
set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

if(MSVC)
    add_compile_options(/std::c++latest)
    set(CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS TRUE)
    set(BUILD_SHARED_LIBS TRUE)
endif()

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# find MPI
find_package(MPI REQUIRED)
if(MPI_FOUND)
    include_directories(SYSTEM ${MPI_INCLUDE_PATH})
endif()

# find boost
set(Boost_USE_STATIC_LIBS ON)
find_package(Boost 1.50 COMPONENTS REQUIRED)
if(Boost_FOUND)
    add_definitions("-DBOOST_ALLOW_DEPRECATED_HEADERS")
    add_definitions("-DBOOST_BIND_GLOBAL_PLACEHOLDERS")
    include_directories(${Boost_INCLUDE_DIRS})
endif()

set(SrcFiles
    Hecatoncheir.cpp

    src/def.cpp
    src/containers.cpp
    src/proc.cpp
    src/utils.cpp
    
    src/env/comm_def.cpp
    src/env/comm.cpp
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

    src/refinement/topology.cpp
)

# add as library
add_library(${PROJECT_NAME} STATIC ${SrcFiles})
target_include_directories(${PROJECT_NAME} PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(${PROJECT_NAME} ${Boost_LIBRARIES} ${MPI_CXX_LIBRARIES})


# agent executable
add_executable(agent agent.cpp)
target_include_directories(agent PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(agent ${PROJECT_NAME} ${Boost_LIBRARIES} ${MPI_CXX_LIBRARIES})

# controller executable
add_executable(controller controller.cpp)
target_include_directories(controller PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(controller ${PROJECT_NAME} ${Boost_LIBRARIES} ${MPI_CXX_LIBRARIES})


# resources dir
set(RESOURCES_DIR "${CMAKE_SOURCE_DIR}/Hecatoncheir/resources")
# build destination for resources dir
set(RESOURCES_DEST_DIR "${CMAKE_BINARY_DIR}/Hecatoncheir/resources")
# copy
add_custom_target(copy_resources ALL
    COMMAND ${CMAKE_COMMAND} -E copy_directory
    ${RESOURCES_DIR} ${RESOURCES_DEST_DIR}
)
add_dependencies(agent copy_resources)
add_dependencies(controller copy_resources)