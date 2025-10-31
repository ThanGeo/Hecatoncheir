# Driver executable
add_executable(driver driver.cpp)

# Include directories
target_include_directories(driver PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../Hecatoncheir/API
)

# Link dependencies - Hecatoncheir should already have MPI linked
target_link_libraries(driver PRIVATE Hecatoncheir)