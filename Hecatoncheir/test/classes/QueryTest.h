#ifndef HECTONCHEIR_QUERY_TEST_H
#define HECTONCHEIR_QUERY_TEST_H

#include <../BaseTest.h>

class QueryTest : public BaseTest {
private:
    std::vector<std::pair<std::string, TestFunc>> tests = {
        {"test1", [this]() { test1(); }},
        {"test2", [this]() { test2(); }},
        {"test3", [this]() { test3(); }},
        {"test4", [this]() { test4(); }},
        {"test5", [this]() { test5(); }},
        {"test6", [this]() { test6(); }},
        {"test7", [this]() { test7(); }}
    };
protected:
    /** @brief Spatial find relation count join between polygons */
    void test1();
    /** @brief Spatial intersection collect join between polygons */
    void test2();
    /** @brief Spatial equal join collect between polygons-linestrings */
    void test3();
    /** @brief Batch knn on points */
    void test4();
    /** @brief Batch range on points count */
    void test5();
    /** @brief Batch range collect on polygons */
    void test6();
    /** @brief Distance join collect on points */
    void test7();

    /** @brief Performs any preparation required for the test */
    void prepare();
    /** @brief Performs any termination tasks for the test */
    void fin();
public:
    QueryTest(const std::vector<std::string>& nodes) : BaseTest(nodes) {}

    void run() override;
};




#endif