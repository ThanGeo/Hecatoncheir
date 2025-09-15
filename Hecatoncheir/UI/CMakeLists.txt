# ignore BOOST deprecated headers
add_definitions("-DBOOST_ALLOW_DEPRECATED_HEADERS")

set(CMAKE_CXX_FLAGS_DEBUG "-g -O1 -fsanitize=address,leak -fno-omit-frame-pointer -fno-common")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

# add the executable
add_executable(
    hec_gui backend/hec_gui.cpp
)

# include the header directories
target_include_directories(hec_gui PRIVATE 
    ${CMAKE_CURRENT_SOURCE_DIR}/
    ${CMAKE_CURRENT_SOURCE_DIR}/backend
)

# link Hecatoncheir
target_link_libraries(hec_gui Hecatoncheir ${MPI_CXX_LIBRARIES})
