#ifndef D_PARSE_H
#define D_PARSE_H

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>

#include "containers.h"
#include "env/partitioning.h"
#include "statement.h"
#include "configure.h"

#include <unordered_map>

/** @brief System parameter and runtime options parsing methods. */
namespace parser
{
    /** @brief Custom hash function for std::pair<QueryType, DataType> objects. */
    struct pair_hash {
        template <class T1, class T2>
        std::size_t operator () (const std::pair<T1, T2>& p) const {
            auto h1 = std::hash<T1>{}(p.first);
            auto h2 = std::hash<T2>{}(p.second);
            return h1 ^ (h2 << 1); // Combine the two hash values
        }
    };

    /** @typedef DataTypePair @brief Defines a pair of data types combination. */
    using DataTypePair = std::pair<DataType, DataType>;
    /** @typedef SupportStatus @brief Map that defines the supported data type combinations. */
    using SupportStatus = std::unordered_map<DataTypePair, bool, pair_hash>;
    /** @typedef SupportStatus @brief Map that defines the supported query types for various data type pairs. */
    using QuerySupportMap = std::unordered_map<QueryType, SupportStatus>;

    extern QuerySupportMap g_querySupportMap;

    /**
    @brief Load config options and parse any cmd arguments.
     * @param[in] argc Number of input arguments.
     * @param[in] argv Input arguments that define runtime options such as datasets, query types etc.
    */
    DB_STATUS parse(int argc, char *argv[]);
}

#endif