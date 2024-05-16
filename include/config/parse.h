#ifndef D_PARSE_H
#define D_PARSE_H


#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>

#include "def.h"
#include "containers.h"

namespace parser
{
    typedef struct SystemOptionsStatement {
        SystemSetupTypeE setupType = SYS_LOCAL_MACHINE;
        std::string nodefilePath;
        uint nodeCount;
    } SystemOptionsStatementT;

    /**
     * @brief Load config options and parse any cmd arguments.
     * @return fills the sysOps object with data
    */
    DB_STATUS parse(int argc, char *argv[], SystemOptionsT &sysOps);
}

#endif