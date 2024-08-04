#ifndef D_PARSE_H
#define D_PARSE_H

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>

#include "containers.h"
#include "env/partitioning.h"
#include "statement.h"
#include "configure.h"

#include <unordered_map>

namespace parser
{
    // Custom hash function for std::pair<QueryType, DataType>
    struct pair_hash {
        template <class T1, class T2>
        std::size_t operator () (const std::pair<T1, T2>& p) const {
            auto h1 = std::hash<T1>{}(p.first);
            auto h2 = std::hash<T2>{}(p.second);
            return h1 ^ (h2 << 1); // Combine the two hash values
        }
    };

    using DataTypePair = std::pair<DataTypeE, DataTypeE>;
    using SupportStatus = std::unordered_map<DataTypePair, bool, pair_hash>;
    using QuerySupportMap = std::unordered_map<QueryTypeE, SupportStatus>;

    extern QuerySupportMap g_querySupportMap;

    /**
     * @brief Load config options and parse any cmd arguments.
     * @return fills the sysOps object with data
    */
    DB_STATUS parse(int argc, char *argv[]);
}

#endif