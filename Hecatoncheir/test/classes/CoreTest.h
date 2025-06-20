#ifndef HECTONCHEIR_CORE_TEST_H
#define HECTONCHEIR_CORE_TEST_H

#include <../BaseTest.h>

class CoreTest : public BaseTest {
private:
    std::vector<std::pair<std::string, TestFunc>> tests = {
        {"test1", [this]() { test1(); }},
        {"test2", [this]() { test2(); }},
        {"test3", [this]() { test3(); }}
    };
protected:
    void test1();
    void test2();
    void test3();
    /** @brief Performs any preparation required for the test */
    void prepare();
    /** @brief Performs any termination tasks for the test */
    void fin();
public:
    CoreTest(const std::vector<std::string>& nodes) : BaseTest(nodes) {}

    void run() override;
};



#endif