#include "env/pack.h"

namespace pack
{
    std::string serializeStrings(std::vector<std::string> &strings) {
        std::ostringstream oss;
        if (strings.size() == 0) {
            return "";
        }
        oss << strings[0];
        for (int i=1; i<strings.size(); i++) {
            oss << "," << strings[i] ;
        }
        return oss.str();
    }

    DB_STATUS deserializeStrings(std::string &serializedString, std::vector<std::string> &strings) {
        // size_t str_size;
        // std::istringstream iss(serializedString);
        // while (iss >> str_size) {
        //     char* str = new char[str_size];
        //     iss.read(str, str_size);
        //     strings.push_back(std::string(str));
        //     printf("pushed back: %s\n", str);
        //     delete[] str;
        // }
        std::istringstream iss(serializedString);
        std::string token;
        // nickname
        getline(iss, token, ',');
        strings.push_back(std::string(token));
        // datatype
        getline(iss, token, ',');
        strings.push_back(std::string(token));




        return DBERR_OK;
    }

    DB_STATUS packDatasetInfo(spatial_lib::DatasetT* dataset, MsgPackT<char> &datasetInfoPack) {      
        std::vector<std::string> datasetInfo;
        datasetInfo.emplace_back(dataset->nickname);
        datasetInfo.emplace_back(spatial_lib::dataTypeIntToText(dataset->dataType));
        // serialize
        std::string serializedStr = serializeStrings(datasetInfo);

        int length = serializedStr.length()+1;
        datasetInfoPack.data = (char*) malloc(length * sizeof(char));
        strncpy(datasetInfoPack.data, serializedStr.c_str(), length);
        datasetInfoPack.count = length;

        return DBERR_OK;
    }

    DB_STATUS packGeometryArrays(std::vector<int> &infoArray, std::vector<double> &coordsArray, MsgPackT<int> &infoPack, MsgPackT<double> &coordsPack) {
        infoPack.data = infoArray.data();
        infoPack.count = infoArray.size();
        coordsPack.data = coordsArray.data();
        coordsPack.count = coordsArray.size();

        return DBERR_OK;
    }

    DB_STATUS packPolygon(spatial_lib::PolygonT &polygon, int partitionID, MsgPackT<int> &infoPack, MsgPackT<double> &coordsPack) {
        // polygon info pack
        infoPack.type = MPI_INT;
        infoPack.count = 3;
        infoPack.data = (int*) malloc(infoPack.count * sizeof(int));
        infoPack.data[0] = polygon.recID;
        infoPack.data[1] = partitionID;
        int vertexNum = polygon.vertices.size();
        infoPack.data[2] = vertexNum;
        
        // polygon coords pack
        coordsPack.type = MPI_DOUBLE;
        std::vector<double> coordVals;
        coordVals.reserve(vertexNum*2);
        for(auto &it : polygon.vertices) {
            coordVals.emplace_back(it.x);
            coordVals.emplace_back(it.y);
        }
        coordsPack.count = coordVals.size();
        coordsPack.data = (double*) malloc(coordsPack.count * sizeof(double));
        std::copy(coordVals.begin(), coordVals.end(), coordsPack.data);

        return DBERR_OK;
    }
}

namespace unpack
{
    DB_STATUS unpackDatasetInfoPack(MsgPackT<char> &datasetInfoPack) {
        // deserialize/unpack
        std::string serializedString(datasetInfoPack.data);
        std::vector<std::string> datasetInfo;
        DB_STATUS ret = pack::deserializeStrings(serializedString, datasetInfo);
        if (ret != DBERR_OK) {
            return ret;
        }

        // build dataset object
        spatial_lib::DatasetT dataset;
        dataset.nickname = datasetInfo[0];
        dataset.dataType = spatial_lib::dataTypeTextToInt(datasetInfo[1]);

        if (dataset.dataType == spatial_lib::DT_INVALID) {
            logger::log_error(DBERR_INVALID_DATATYPE, "Unpacking dataset info returned invalid data type. Pack datatype:", datasetInfo[1]);
            return DBERR_INVALID_DATATYPE;
        }

        // add to configuration
        g_config.datasetInfo.addDataset(dataset);

        return ret;
    }
}