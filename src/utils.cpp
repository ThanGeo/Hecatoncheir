#include "utils.h"

bool verifyFilepath(std::string filePath) {
    if (FILE *file = fopen(filePath.c_str(), "r")) {
        fclose(file);
        return true;
    }
    return false;
}

bool verifyDirectory(std::string directoryPath) {
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

std::string getDatasetNameFromPath(std::string &datasetPath) {
    std::stringstream ss(datasetPath);
    std::string token;

    // set delimiter of directories
    std::size_t found = datasetPath.find('/');
    char delimiter;
    if (found!=std::string::npos){
        delimiter = '/';
    } else if (datasetPath.find('\\')) {
        delimiter = '\\';
    } else {
        // no delimiter
        delimiter = '\0';
    }

    if (delimiter != '\0') {
        while(std::getline(ss, token, delimiter)) {
            // do nothing
        }
    }
    // last item is stored in variable token
    std::string datasetName = token.substr(0, token.length() - 4);
    return datasetName;
}

bool binarySearchInIntervalVector(std::vector<uint32_t> &vec, uint32_t x){
    int low = 0;
    int high = vec.size()-1;
    int mid;
    while(low <= high){
        mid = (low+high) / 2;
        if(vec.at(mid) > x){
            if(mid-1 > 0){
                if(vec.at(mid-1) <= x){
                    return !((mid-1)%2);
                }
            }
            high = mid-1;
        }else{
            if(mid+1 > vec.size()-1 || vec.at(mid+1) > x){
                return !(mid%2);
            }
            low = mid+1;
        }
    }
    return false;
}

bool binarySearchInVector(std::vector<uint32_t> &vec, uint32_t &x){
    int low = 0;
    int high = vec.size()-1;
    int mid;
    while(low <= high){
        mid = (low+high) / 2;
        if(vec.at(mid) == x){
            return true;
        }
        if(vec.at(mid) < x){
            low = mid+1;
        }else{
            high = mid-1;
        }
    }
    return false;
}

namespace mpi_timer
{
    double markTime() {
        return MPI_Wtime();
    }

    double getElapsedTime(double startTime) {
        return MPI_Wtime() - startTime;
    }
}