# Test executable
add_executable(test test.cpp classes/CoreTest.cpp classes/QueryTest.cpp)

# Include directories
target_include_directories(test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/classes
)

# Add definitions
target_compile_definitions(test PRIVATE BOOST_ALLOW_DEPRECATED_HEADERS)

# Link dependencies
target_link_libraries(test PRIVATE Hecatoncheir)