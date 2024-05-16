#include "utils.h"

bool verifyFileExists(std::string filePath) {
    if (FILE *file = fopen(filePath.c_str(), "r")) {
        fclose(file);
        return true;
    }
    return false;
}

bool verifyDirectoryExists(std::string directoryPath) {
    //check if APRIL dirs exists
    DIR* dir = opendir(directoryPath.c_str());
    if(dir) {
        /* Directory exists. */
        closedir(dir);
        return true;
    }else if(ENOENT == errno) {
        /* Directory does not exist. */
        return false;
    }else{
        /* opendir() failed for some other reason. */
        return false;
    }
}