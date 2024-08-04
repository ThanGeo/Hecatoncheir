cmake_minimum_required(VERSION 3.0.0)
project(Main VERSION 1.0.0)

# find MPI
find_package(MPI REQUIRED)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

set(CMAKE_CXX_FLAGS "-fopenmp")
set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

if(MSVC)
    add_compile_options(/std::c++latest)
    set(CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS TRUE)
    set(BUILD_SHARED_LIBS TRUE)
endif()

# supress warnings
# add_definitions(-w)

set(SrcFiles
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

# supress warnings
# add_definitions(-w)
include_directories(include)
add_library(${PROJECT_NAME} ${ProjectType} ${SrcFiles})

# find boost
set(Boost_USE_STATIC_LIBS ON)
find_package(Boost 1.50 COMPONENTS REQUIRED)
if(Boost_FOUND)
    add_definitions("-DBOOST_ALLOW_DEPRECATED_HEADERS")
    add_definitions("-DBOOST_BIND_GLOBAL_PLACEHOLDERS")
    include_directories(${Boost_INCLUDE_DIRS})
endif()

if(MPI_FOUND)
    include_directories(SYSTEM ${MPI_INCLUDE_PATH})
endif()

#main
add_executable(agent agent.cpp)
add_executable(controller controller.cpp)

# link all
target_link_libraries(controller ${PROJECT_NAME})
target_link_libraries(controller ${Boost_LIBRARIES})
target_link_libraries(controller ${MPI_C_LIBRARIES})
target_link_libraries(agent ${PROJECT_NAME})
target_link_libraries(agent ${Boost_LIBRARIES})
target_link_libraries(agent ${MPI_C_LIBRARIES})