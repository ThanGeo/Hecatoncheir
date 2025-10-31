cmake_minimum_required(VERSION 3.22)
project(Root)

# Set C++ standard FIRST (before any targets)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Compiler flags (consider using target_compile_options instead)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fopenmp")
# set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O1 -fsanitize=address,leak -fno-omit-frame-pointer -fno-common")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")

# MSVC settings
if(MSVC)
    set(CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS TRUE)
    set(BUILD_SHARED_LIBS TRUE)
    # MSVC equivalent of -fopenmp
    add_compile_options(/openmp)
endif()

# Default build type
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# Find dependencies ONCE at top level
find_package(MPI REQUIRED)
find_package(Boost 1.71 REQUIRED)

add_subdirectory(Hecatoncheir)
add_subdirectory(driver)