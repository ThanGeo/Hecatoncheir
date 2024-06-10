#ifndef D_PARSE_H
#define D_PARSE_H


#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>

#include "SpatialLib.h"
#include "def.h"
#include "containers.h"
#include "env/partitioning.h"
#include "statement.h"
#include "configure.h"


#include <unordered_map>




namespace parser
{
    /**
     * @brief Load config options and parse any cmd arguments.
     * @return fills the sysOps object with data
    */
    DB_STATUS parse(int argc, char *argv[]);
}

#endif