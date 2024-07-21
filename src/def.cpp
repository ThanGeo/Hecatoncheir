#include "def.h"

Config g_config;

QueryOutputT g_queryOutput;

int g_world_size;
int g_node_rank;
int g_host_rank = HOST_RANK;
int g_parent_original_rank;

MPI_Comm g_global_comm = MPI_COMM_WORLD;
MPI_Comm g_local_comm;



void queryResultReductionFunc(QueryOutputT &in, QueryOutputT &out) {
    out.queryResults += in.queryResults;
    out.postMBRFilterCandidates += in.postMBRFilterCandidates;
    out.refinementCandidates += in.refinementCandidates;
    switch (g_config.queryInfo.type) {
        case Q_DISJOINT:
        case Q_INTERSECT:
        case Q_INSIDE:
        case Q_CONTAINS:
        case Q_COVERS:
        case Q_COVERED_BY:
        case Q_MEET:
        case Q_EQUAL:
            out.trueHits += in.trueHits;
            out.trueNegatives += in.trueNegatives;
            break;
        case Q_FIND_RELATION:
            for (auto &it : in.topologyRelationsResultMap) {
                out.topologyRelationsResultMap[it.first] += it.second;
            }
            break;
    }
}

std::vector<int> getCommonSectionIDsOfObjects(Dataset *datasetR, Dataset *datasetS, size_t idR, size_t idS) {
    auto itR = datasetR->recToSectionIdMap.find(idR);
    auto itS = datasetS->recToSectionIdMap.find(idS);
    std::vector<int> commonSectionIDs;
    set_intersection(itR->second.begin(),itR->second.end(),itS->second.begin(),itS->second.end(),back_inserter(commonSectionIDs));
    return commonSectionIDs;
}

QueryOutput::QueryOutput() {
    // result
    this->queryResults = 0;
    // topology relations results
    this->topologyRelationsResultMap.clear();
    this->topologyRelationsResultMap.insert(std::make_pair(TR_DISJOINT, 0));
    this->topologyRelationsResultMap.insert(std::make_pair(TR_EQUAL, 0));
    this->topologyRelationsResultMap.insert(std::make_pair(TR_MEET, 0));
    this->topologyRelationsResultMap.insert(std::make_pair(TR_CONTAINS, 0));
    this->topologyRelationsResultMap.insert(std::make_pair(TR_COVERS, 0));
    this->topologyRelationsResultMap.insert(std::make_pair(TR_COVERED_BY, 0));
    this->topologyRelationsResultMap.insert(std::make_pair(TR_INSIDE, 0));
    this->topologyRelationsResultMap.insert(std::make_pair(TR_INTERSECT, 0));
    // statistics
    this->postMBRFilterCandidates = 0;
    this->refinementCandidates = 0;
    this->trueHits = 0;
    this->trueNegatives = 0;
    
    // time
    this->mbrFilterTime = 0;
    this->iFilterTime = 0;
    this->refinementTime = 0;

    // on the fly april
    this->rasterizationsDone = 0;
}

void QueryOutput::reset() {
    // result
    this->queryResults = 0;
    // topology relations results
    this->topologyRelationsResultMap.clear();
    this->topologyRelationsResultMap.insert(std::make_pair(TR_DISJOINT, 0));
    this->topologyRelationsResultMap.insert(std::make_pair(TR_EQUAL, 0));
    this->topologyRelationsResultMap.insert(std::make_pair(TR_MEET, 0));
    this->topologyRelationsResultMap.insert(std::make_pair(TR_CONTAINS, 0));
    this->topologyRelationsResultMap.insert(std::make_pair(TR_COVERS, 0));
    this->topologyRelationsResultMap.insert(std::make_pair(TR_COVERED_BY, 0));
    this->topologyRelationsResultMap.insert(std::make_pair(TR_INSIDE, 0));
    this->topologyRelationsResultMap.insert(std::make_pair(TR_INTERSECT, 0));
    // statistics
    this->postMBRFilterCandidates = 0;
    this->refinementCandidates = 0;
    this->trueHits = 0;
    this->trueNegatives = 0;
    
    // time
    this->mbrFilterTime = 0;
    this->iFilterTime = 0;
    this->refinementTime = 0;

    // on the fly april
    this->rasterizationsDone = 0;
}

void QueryOutput::countAPRILresult(int result) {
    switch (result) {
        case TRUE_HIT:
            this->trueHits += 1;
            this->queryResults += 1;
            break;
        case TRUE_NEGATIVE:
            this->trueNegatives += 1;
            break;
        case INCONCLUSIVE:
            this->refinementCandidates += 1;
            break;
    }
}

void QueryOutput::countResult(){
    this->queryResults += 1;
}

void QueryOutput::countMBRresult(){
    this->postMBRFilterCandidates += 1;
}

void QueryOutput::countRefinementCandidate(){
    this->refinementCandidates += 1;
}

void QueryOutput::countTopologyRelationResult(int relation) {
    this->topologyRelationsResultMap[relation] += 1;
}

int QueryOutput::getResultForTopologyRelation(TopologyRelationE relation) {
    auto it = topologyRelationsResultMap.find(relation);
    if (it != topologyRelationsResultMap.end()) {
        return it->second;
    }
    return -1;
}

void QueryOutput::setTopologyRelationResult(int relation, int result) {
    auto it = topologyRelationsResultMap.find(relation);
    if (it != topologyRelationsResultMap.end()) {
        // exists already, replace
        it->second = result;
    } else {
        // new entry
        topologyRelationsResultMap.insert(std::make_pair(relation, result));
    }
}

void QueryOutput::shallowCopy(QueryOutput &other) {
    // result
    this->queryResults = other.queryResults;
    // topology relations results
    this->topologyRelationsResultMap.clear();
    for (auto &it: other.topologyRelationsResultMap) {
        this->topologyRelationsResultMap.insert(std::make_pair(it.first, it.second));
    }
    // statistics
    this->postMBRFilterCandidates = other.postMBRFilterCandidates;
    this->refinementCandidates = other.refinementCandidates;
    this->trueHits = other.trueHits;
    this->trueNegatives = other.trueNegatives;
    
    // time
    this->mbrFilterTime = other.mbrFilterTime;
    this->iFilterTime = other.iFilterTime;
    this->refinementTime = other.refinementTime;

    // on the fly april
    this->rasterizationsDone = other.rasterizationsDone;
}

int Dataset::calculateBufferSize() {
    int size = 0;
    // dataset data type
    size += sizeof(DataTypeE);
    // dataset nickname: length + string
    size += sizeof(int) + (nickname.length() * sizeof(char));     
    // dataset's dataspace info (MBR)
    size += 4 * sizeof(double);
    
    return size;
}

/**
 * serialize dataset info (only specific stuff)
 */
int Dataset::serialize(char **buffer) {
    int position = 0;
    // calculate size
    int bufferSize = calculateBufferSize();
    // allocate space
    char* localBuffer = (char*) malloc(bufferSize * sizeof(char));

    if (localBuffer == NULL) {
        // malloc failed
        return -1;
    }

    // add datatype
    memcpy(localBuffer + position, &dataType, sizeof(DataTypeE));
    position += sizeof(DataTypeE);
    // add dataset nickname length + string
    int nicknameLength = nickname.length();
    memcpy(localBuffer + position, &nicknameLength, sizeof(int));
    position += sizeof(int);
    memcpy(localBuffer + position, nickname.data(), nicknameLength);
    position += nicknameLength * sizeof(char);
    // add dataset's dataspace MBR
    memcpy(localBuffer + position, &dataspaceInfo.xMinGlobal, sizeof(double));
    position += sizeof(double);
    memcpy(localBuffer + position, &dataspaceInfo.yMinGlobal, sizeof(double));
    position += sizeof(double);
    memcpy(localBuffer + position, &dataspaceInfo.xMaxGlobal, sizeof(double));
    position += sizeof(double);
    memcpy(localBuffer + position, &dataspaceInfo.yMaxGlobal, sizeof(double));
    position += sizeof(double);
    // set and return
    (*buffer) = localBuffer;
    return bufferSize;
}

void Dataset::deserialize(const char *buffer, int bufferSize) {
    int nicknameLength;
    int position = 0;
    double xMin, yMin, xMax, yMax;
    
    // dataset data type
    memcpy(&dataType, buffer + position, sizeof(DataTypeE));
    position += sizeof(DataTypeE);
    // dataset nickname length + string
    memcpy(&nicknameLength, buffer + position, sizeof(int));
    position += sizeof(int);
    nickname.assign(buffer + position, nicknameLength);
    position += nicknameLength * sizeof(char);
    // dataset dataspace MBR
    memcpy(&xMin, buffer + position, sizeof(double));
    position += sizeof(double);
    memcpy(&yMin, buffer + position, sizeof(double));
    position += sizeof(double);
    memcpy(&xMax, buffer + position, sizeof(double));
    position += sizeof(double);
    memcpy(&yMax, buffer + position, sizeof(double));
    position += sizeof(double);
    // set
    dataspaceInfo.set(xMin, yMin, xMax, yMax);

    if (position == bufferSize) {
        // all is well
    }
}

void Dataset::addAprilDataToApproximationDataMap(const uint sectionID, const size_t recID, const AprilDataT &aprilData) {
    // store april data
    this->sectionMap[sectionID].aprilData.insert(std::make_pair(recID, aprilData));
    // store mapping recID -> sectionID
    auto it = this->recToSectionIdMap.find(recID);
    if (it != this->recToSectionIdMap.end()) {
        // exists
        it->second.emplace_back(sectionID);
    } else {
        // doesnt exist, new entry
        std::vector<uint> sectionIDs = {sectionID};
        this->recToSectionIdMap.insert(std::make_pair(recID, sectionIDs));
    }
}

void Dataset::addObjectToSectionMap(const uint sectionID, const size_t recID) {
    AprilDataT aprilData;
    this->sectionMap[sectionID].aprilData.insert(std::make_pair(recID, aprilData));
    // store mapping recID -> sectionID
    auto it = this->recToSectionIdMap.find(recID);
    if (it != this->recToSectionIdMap.end()) {
        // exists
        it->second.emplace_back(sectionID);
    } else {
        // doesnt exist, new entry
        std::vector<uint> sectionIDs = {sectionID};
        this->recToSectionIdMap.insert(std::make_pair(recID, sectionIDs));
    }
}

void Dataset::addIntervalsToAprilData(const uint sectionID, const size_t recID, const int numIntervals, const std::vector<uint32_t> &intervals, const bool ALL) {
    // retrieve section
    auto secIT = this->sectionMap.find(sectionID);
    if (secIT == this->sectionMap.end()) {
        logger::log_error(DBERR_INVALID_PARAMETER, "could not find section ID", sectionID, "in section map of dataset", this->nickname);
        return;
    }
    // retrieve object
    auto it = secIT->second.aprilData.find(recID);
    if (it == secIT->second.aprilData.end()) {
        logger::log_error(DBERR_INVALID_PARAMETER, "could not find recID", recID, "in section map of dataset", this->nickname);
        return;
    }
    // replace intervals in april data
    if (ALL) {
        it->second.numIntervalsALL = numIntervals;
        it->second.intervalsALL = intervals;
    } else {
        it->second.numIntervalsFULL = numIntervals;
        it->second.intervalsFULL = intervals;
    }
}

AprilDataT* Dataset::getAprilDataBySectionAndObjectID(uint sectionID, size_t recID) {
    auto sec = this->sectionMap.find(sectionID);
    if (sec != this->sectionMap.end()){
        auto obj = sec->second.aprilData.find(recID);
        if (obj != sec->second.aprilData.end()) {
            return &(obj->second);
        }
    }
    return nullptr;
}

Dataset* DatasetInfo::getDatasetByNickname(std::string &nickname) {
    auto it = datasets.find(nickname);
    if (it == datasets.end()) {
        return nullptr;
    }        
    return &it->second;
}


namespace shape_factory
{
    // Create empty shapes
    Shape createEmptyPointShape() {
        return Shape(PointWrapper());
    }

    Shape createEmptyPolygonShape() {
        return Shape(PolygonWrapper());
    }

    Shape createEmptyLineStringShape() {
        return Shape(LineStringWrapper());
    }

    Shape createEmptyRectangleShape() {
        return Shape(RectangleWrapper());
    }
}

namespace mapping
{
    std::string actionIntToStr(ActionTypeE action) {
        switch (action) {
            case ACTION_NONE:
                return "NONE";
            case ACTION_LOAD_DATASETS:
                return "LOAD DATASETS";
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

    std::string queryTypeIntToStr(int val){
        switch(val) {
            case Q_INTERSECT: return "intersect";
            case Q_INSIDE: return "inside";
            case Q_DISJOINT: return "disjoint";
            case Q_EQUAL: return "equal";
            case Q_COVERED_BY: return "covered by";
            case Q_COVERS: return "covers";
            case Q_CONTAINS: return "contains";
            case Q_MEET: return "meet";
            case Q_FIND_RELATION: return "find relation";
            default: return "";
        }
    }

    int queryTypeStrToInt(std::string &str) {
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
            return -1;
        }
    }

    std::string dataTypeIntToStr(DataTypeE val){
        switch(val) {
            case DT_POLYGON: return "POLYGON";
            case DT_RECTANGLE: return "RECTANGLE";
            case DT_POINT: return "POINT";
            case DT_LINESTRING: return "LINESTRING";
            default: return "";
        }
    }

    DataTypeE dataTypeTextToInt(std::string str){
        if (str.compare("POLYGON") == 0) return DT_POLYGON;
        else if (str.compare("RECTANGLE") == 0) return DT_RECTANGLE;
        else if (str.compare("POINT") == 0) return DT_POINT;
        else if (str.compare("LINESTRING") == 0) return DT_LINESTRING;

        return DT_INVALID;
    }

    FileTypeE fileTypeTextToInt(std::string str) {
        if (str.compare("BINARY") == 0) return FT_BINARY;
        else if (str.compare("CSV") == 0) return FT_CSV;
        else if (str.compare("WKT") == 0) return FT_WKT;

        return FT_INVALID;
    }

    std::string datatypeCombinationIntToStr(DatatypeCombinationE val) {
        switch(val) {
            case DC_POINT_POINT: return "POINT-POINT";
            case DC_POINT_LINESTRING: return "POINT-LINESTRING";
            case DC_POINT_RECTANGLE: return "POINT-RECTANGLE";
            case DC_POINT_POLYGON: return "POINT-POLYGON";
            
            case DC_LINESTRING_POINT: return "LINESTRING-POINT";
            case DC_LINESTRING_LINESTRING: return "LINESTRING-LINESTRING";
            case DC_LINESTRING_RECTANGLE: return "LINESTRING-RECTANGLE";
            case DC_LINESTRING_POLYGON: return "LINESTRING-POLYGON";

            case DC_RECTANGLE_POINT: return "RECTANGLE-POINT";
            case DC_RECTANGLE_LINESTRING: return "RECTANGLE-LINESTRING";
            case DC_RECTANGLE_RECTANGLE: return "RECTANGLE-RECTANGLE";
            case DC_RECTANGLE_POLYGON: return "RECTANGLE-POLYGON";

            case DC_POLYGON_POINT: return "POLYGON-POINT";
            case DC_POLYGON_LINESTRING: return "POLYGON-LINESTRING";
            case DC_POLYGON_RECTANGLE: return "POLYGON-RECTANGLE";
            case DC_POLYGON_POLYGON: return "POLYGON-POLYGON";
            default: return "";
        }
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