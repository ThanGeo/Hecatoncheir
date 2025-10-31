# ignore BOOST deprecated headers
add_definitions("-DBOOST_ALLOW_DEPRECATED_HEADERS")

set(CMAKE_CXX_FLAGS_DEBUG "-g -O1 -fsanitize=address,leak -fno-omit-frame-pointer -fno-common")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

# Find nlohmann_json
find_package(nlohmann_json REQUIRED)

# add the executable
add_executable(
    hec_server backend/hec_server.cpp
)

# include the header directories
target_include_directories(hec_server PRIVATE 
    ${CMAKE_CURRENT_SOURCE_DIR}/
    ${CMAKE_CURRENT_SOURCE_DIR}/backend
)

# link Hecatoncheir and nlohmann_json
target_link_libraries(hec_server Hecatoncheir ${MPI_CXX_LIBRARIES} nlohmann_json::nlohmann_json)
