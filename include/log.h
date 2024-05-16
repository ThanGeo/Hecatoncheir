#ifndef D_LOG_H
#define D_LOG_H

#include <iostream>

namespace log
{
    // Base case of the variadic template recursion
    inline void report_error(const std::string& error_msg) {
        std::cout << std::endl;
    }

    // Recursive template function to print each argument
    template<typename T, typename... Args>
    inline void report_error(const std::string& error_msg, T first, Args... rest) {
        std::cout << "Error: " << error_msg << " - ";
        report_error(first, rest...);
    }
}



#endif