#ifndef HECTONCHEIR_TEST_H
#define HECTONCHEIR_TEST_H

#include <../API/Hecatoncheir.h>
#include "../include/def.h"
#include "../include/utils.h"

#define ASSERT_EQ(expected, actual) \
    do { \
        if ((expected) != (actual)) { \
            std::ostringstream oss; \
            oss << current_test_name << ": " \
                << #expected << " == " << #actual \
                << " (" << expected << " != " << actual << ")"; \
            throw std::runtime_error(oss.str()); \
        } \
    } while (0)

#define ASSERT_NE(val1, val2) \
    do { \
        if ((val1) == (val2)) { \
            std::ostringstream oss; \
            oss << current_test_name << ": " \
                << #val1 << " != " << #val2 \
                << " (" << val1 << " == " << val2 << ")"; \
            throw std::runtime_error(oss.str()); \
        } \
    } while (0)

using TestFunc = std::function<void()>;
inline thread_local std::string current_test_name;

class BaseTest {
protected:
    std::vector<std::string> nodes;
public:
    BaseTest(std::vector<std::string> nodes) {
        this->nodes = nodes;
    }

    static void init(std::vector<std::string> nodes) {
        // initialize
        int ret = hec::init(nodes.size(), nodes);
        ASSERT_EQ(DBERR_OK, ret);
    }

    static void fin() {
        // finalize hec
        int ret = hec::finalize();
        ASSERT_EQ(DBERR_OK, ret);
    }

    virtual ~BaseTest() = default;

    /** @brief Executes the test */
    virtual void run() = 0;
};


#endif