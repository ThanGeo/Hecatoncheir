#include "utils.h"

namespace hilbert
{
    /**
     * rotate/flip a quadrant appropriately
     */
    static inline void rot(uint32_t n, uint32_t &x, uint32_t &y, uint32_t rx, uint32_t ry) {
        if (ry == 0) {
            if (rx == 1) {
                x = n-1 - x;
                y = n-1 - y;
            }
            //Swap x and y
            uint32_t t  = x;
            x = y;
            y = t;
        }
    }

    uint32_t xy2d (uint32_t n, uint32_t x, uint32_t y) {
        uint32_t rx, ry, s;
        uint32_t d=0;
        for (s=n/2; s>0; s/=2) {
            rx = (x & s) > 0;
            ry = (y & s) > 0;
            d += s * s * ((3 * rx) ^ ry);
            rot(s, x, y, rx, ry);
        }
        return d;
    }

    void d2xy(uint32_t n, uint32_t d, uint32_t &x, uint32_t &y) {
        uint32_t rx, ry, s, t=d;
        x = y = 0;
        for (s=1; s<n; s*=2) {
            rx = 1 & (t/2);
            ry = 1 & (t ^ rx);
            rot(s, x, y, rx, ry);
            x += s * rx;
            y += s * ry;
            t /= 4;
        }
    }
}
/**
 * @todo: add the utils methods in a utils namespace
 */
DB_STATUS verifyFilepath(std::string filePath) {
    if (FILE *file = fopen(filePath.c_str(), "r")) {
        fclose(file);
        return DBERR_OK;
    }
    return DBERR_INVALID_FILE_PATH;
}

DB_STATUS verifyDirectory(std::string directoryPath) {
    //check if APRIL dirs exists
    DIR* dir = opendir(directoryPath.c_str());
    if(dir) {
        /* Directory exists. */
        closedir(dir);
        return DBERR_OK;
    }else if(ENOENT == errno) {
        /* Directory does not exist. */
        return DBERR_DIR_NOT_EXIST;
    }else{
        /* opendir() failed for some other reason. */
        return DBERR_OPEN_DIR_FAILED;
    }
}

std::string getFileNameFromPath(std::string &filePath) {
    std::stringstream ss(filePath);
    std::string token;

    // set delimiter of directories
    std::size_t found = filePath.find('/');
    char delimiter;
    if (found!=std::string::npos){
        delimiter = '/';
    } else if (filePath.find('\\')) {
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
    std::string fileName = token.substr(0, token.length() - 4);
    return fileName;
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

namespace mapping
{
    std::string actionIntToStr(ActionType action) {
        switch (action) {
            case ACTION_NONE:
                return "NONE";
            case ACTION_LOAD_DATASET_R:
                return "LOAD DATASET R";
            case ACTION_LOAD_DATASET_S:
                return "LOAD DATASET S";
            case ACTION_PERFORM_PARTITIONING:
                return "PERFORM PARTITIONING";
            case ACTION_CREATE_APRIL:
                return "CREATE APRIL";
            case ACTION_PERFORM_VERIFICATION:
                return "PERFORM VERIFICATION";
            case ACTION_QUERY:
                return "QUERY";
            case ACTION_LOAD_APRIL:
                return "LOAD APRIL";
            default:
                return "";
        }
    }

    std::string twoLayerClassIntToStr(TwoLayerClass classType) {
        switch (classType) {
            case CLASS_A:
                return "A";
            case CLASS_B:
                return "B";
            case CLASS_C:
                return "C";
            case CLASS_D:
                return "D";
            default:
                return "NO CLASS";
        }
    }

    std::string queryTypeIntToStr(QueryType val){
        switch(val) {
            case Q_RANGE: return "range";
            case Q_INTERSECT: return "intersect";
            case Q_INSIDE: return "inside";
            case Q_DISJOINT: return "disjoint";
            case Q_EQUAL: return "equal";
            case Q_COVERED_BY: return "covered by";
            case Q_COVERS: return "covers";
            case Q_CONTAINS: return "contains";
            case Q_MEET: return "meet";
            case Q_FIND_RELATION: return "find relation";
            case Q_NONE: return "no query";
            default: return "";
        }
    }

    QueryType queryTypeStrToInt(std::string &str) {
        if (str == "range") {
            return Q_RANGE;
        } else if (str == "intersect") {
            return Q_INTERSECT;
        } else if (str == "inside") {
            return Q_INSIDE;
        } else if (str == "disjoint") {
            return Q_DISJOINT;
        } else if (str == "equal") {
            return Q_EQUAL;
        } else if (str == "meet") {
            return Q_MEET;
        } else if (str == "contains") {
            return Q_CONTAINS;
        } else if (str == "covered_by") {
            return Q_COVERED_BY;
        } else if (str == "covers") {
            return Q_COVERS;
        } else if (str == "find_relation") {
            return Q_FIND_RELATION;
        } else {
            return Q_NONE;
        }
    }

    std::string dataTypeIntToStr(DataType val){
        switch(val) {
            case DT_POLYGON: return "POLYGON";
            case DT_RECTANGLE: return "RECTANGLE";
            case DT_POINT: return "POINT";
            case DT_LINESTRING: return "LINESTRING";
            default: return "";
        }
    }

    DataType dataTypeTextToInt(std::string str){
        if (str.compare("POLYGON") == 0) return DT_POLYGON;
        else if (str.compare("RECTANGLE") == 0) return DT_RECTANGLE;
        else if (str.compare("POINT") == 0) return DT_POINT;
        else if (str.compare("LINESTRING") == 0) return DT_LINESTRING;

        return (DataType)(-1);
    }

    FileType fileTypeTextToInt(std::string str) {        
        if (str.compare("CSV") == 0) return FT_CSV;
        else if (str.compare("WKT") == 0) return FT_WKT;

        return (FileType)(-1);
    }

    std::string relationIntToStr(int relation) {
        switch(relation) {
            case TR_INTERSECT: return "INTERSECT";
            case TR_CONTAINS: return "CONTAINS";
            case TR_DISJOINT: return "DISJOINT";
            case TR_EQUAL: return "EQUAL";
            case TR_COVERS: return "COVERS";
            case TR_MEET: return "MEET";
            case TR_COVERED_BY: return "COVERED BY";
            case TR_INSIDE: return "INSIDE";
            default: return "";
        }
    }
}