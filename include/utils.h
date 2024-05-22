#ifndef D_UTILS_H
#define D_UTILS_H

#include <dirent.h>
#include <string>
#include <sstream>

bool verifyFileExists(std::string filePath);
bool verifyDirectoryExists(std::string directoryPath);
std::string getDatasetNameFromPath(std::string &datasetPath);


#endif