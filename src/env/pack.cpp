#include "env/pack.h"

namespace pack
{

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