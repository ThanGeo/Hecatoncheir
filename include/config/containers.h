#ifndef D_CONFIG_CONTAINERS_H
#define D_CONFIG_CONTAINERS_H

#include "def.h"


typedef enum SystemSetupType {
    SYS_LOCAL_MACHINE,
    SYS_CLUSTER,
} SystemSetupTypeE;

typedef struct SystemOptions {
    SystemSetupTypeE setupType;
    std::string nodefilePath;
    uint nodeCount;
} SystemOptionsT;



#endif