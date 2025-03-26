#include "APRIL/generate.h"

namespace APRIL
{
    namespace generation 
    {
        DataspaceMetadata rasterDataspaceMetadata;
        std::vector<int> x_offset = { 1,1,1, 0,0,-1,-1,-1};
        std::vector<int> y_offset = {-1,0,1,-1,1,-1, 0, 1};

        DB_STATUS setRasterBounds(DataspaceMetadata &dataspaceMetadata) {
            if (dataspaceMetadata.xMinGlobal >= dataspaceMetadata.xMaxGlobal || dataspaceMetadata.yMinGlobal >= dataspaceMetadata.yMaxGlobal) {
                logger::log_error(DBERR_INVALID_PARAMETER, "Invalid raster bounds. min values must be smaller than the max values. Bounds:", dataspaceMetadata.xMinGlobal, dataspaceMetadata.yMinGlobal, dataspaceMetadata.xMaxGlobal, dataspaceMetadata.yMaxGlobal);
                return DBERR_INVALID_PARAMETER;
            }
            rasterDataspaceMetadata = dataspaceMetadata;
            // logger::log_success("Raster bounds set:", dataspaceMetadata.xMinGlobal, dataspaceMetadata.yMinGlobal, dataspaceMetadata.xMaxGlobal, dataspaceMetadata.yMaxGlobal);
            return DBERR_OK;
        }
        

        static inline bool checkY(double &y1, double &OGy1, double &OGy2, uint32_t &startCellY, uint32_t &endCellY){
            if(OGy1 < OGy2){
                return (startCellY <= y1 && y1 <= endCellY + 1);
            }else{
                return (endCellY <= y1 && y1 <= startCellY + 1);
            }
        }

        static inline double mapSingleValueToHilbert(double val, double minval, double maxval, uint32_t &cellsPerDim){
            double newval =  ((double) (cellsPerDim-1) / (maxval - minval)) * (val - minval);
            if(newval < 0){
                newval = 0;
            }
            if(newval >= cellsPerDim){
                newval = cellsPerDim-1;
            }
            return newval;
        }

        static inline void mapXYToHilbert(double &x, double &y, uint32_t &cellsPerDim){
            x = ((double) (cellsPerDim-1) / (rasterDataspaceMetadata.xMaxGlobal - rasterDataspaceMetadata.xMinGlobal)) * (x - rasterDataspaceMetadata.xMinGlobal);
            y = ((double) (cellsPerDim-1) / (rasterDataspaceMetadata.yMaxGlobal - rasterDataspaceMetadata.yMinGlobal)) * (y - rasterDataspaceMetadata.yMinGlobal);
            if(x < 0){
                x = 0;
            }
            if(y < 0){
                y = 0;
            }
            if(x >= cellsPerDim){
                x = cellsPerDim-1;
            }
            if(y >= cellsPerDim){
                y = cellsPerDim-1;
            }
        }

        static inline int checkNeighborsInInterval(uint32_t &current_id, uint32_t &x, uint32_t &y, RasterData &rasterData, std::vector<uint32_t> &fullIntervals, std::vector<uint32_t> &partialCells, uint32_t cellsPerDim){
            if(fullIntervals.size() == 0){
                return UNCERTAIN;
            }
            uint32_t d;
            //4 neighbors
            for(int i=0; i<x_offset.size(); i++){		
                //if neighbor is out of bounds, ignore
                if(x+x_offset.at(i) < rasterData.minCellX || x+x_offset.at(i) > rasterData.maxCellX || y+y_offset.at(i) < rasterData.minCellY || y+y_offset.at(i) > rasterData.maxCellY){
                    continue;
                }

                //get hilbert cell id
                d = hilbert::xy2d(cellsPerDim, x+x_offset.at(i), y+y_offset.at(i));
                //if it has higher hilbert order, ignore
                if(d >= current_id){
                    continue;
                }
                // if its a partial cell, ignore
                if(binarySearchInVector(partialCells, d)){
                    //partial
                    continue;
                }
                //check if it is full
                if(binarySearchInIntervalVector(fullIntervals, d)){
                    //full
                    return FULL;
                }else{
                    //else its empty
                    return EMPTY;
                }
            }
            return UNCERTAIN;
        }

        static inline DB_STATUS mapObjectToHilbertSpace(Shape &object, RasterData &rasterData, uint32_t cellsPerDim){
            //first, map the object's coordinates to this section's hilbert space
            double x,y;
            const std::vector<bg_point_xy>* vertices = object.getReferenceToPoints();
            
            if (vertices == nullptr) {
                logger::log_error(DBERR_NULL_PTR_EXCEPTION, "Empty vertex list returned for object with id", object.recID);
                return DBERR_NULL_PTR_EXCEPTION;
            }
            for (int i=0; i<vertices->size(); i++) {
                x = vertices->at(i).x();
                y = vertices->at(i).y();
                // printf("mapping %f,%f to ", x, y);
                mapXYToHilbert(x, y, cellsPerDim);
                object.modifyBoostPointByIndex(i, x, y);
                // printf("%f,%f\n", x, y);
            }
            object.correctGeometry();
            //map the object's mbr
            rasterData.minCellX = mapSingleValueToHilbert(object.mbr.pMin.x, rasterDataspaceMetadata.xMinGlobal, rasterDataspaceMetadata.xMaxGlobal, cellsPerDim);
            rasterData.minCellX > 0 ? rasterData.minCellX -= 1 : rasterData.minCellX = 0;
            rasterData.minCellY = mapSingleValueToHilbert(object.mbr.pMin.y, rasterDataspaceMetadata.yMinGlobal, rasterDataspaceMetadata.yMaxGlobal, cellsPerDim);
            rasterData.minCellY > 0 ? rasterData.minCellY -= 1 : rasterData.minCellY = 0;
            rasterData.maxCellX = mapSingleValueToHilbert(object.mbr.pMax.x, rasterDataspaceMetadata.xMinGlobal, rasterDataspaceMetadata.xMaxGlobal, cellsPerDim);
            rasterData.maxCellX < cellsPerDim - 1 ? rasterData.maxCellX += 1 : rasterData.maxCellX = cellsPerDim - 1;
            rasterData.maxCellY = mapSingleValueToHilbert(object.mbr.pMax.y, rasterDataspaceMetadata.yMinGlobal, rasterDataspaceMetadata.yMaxGlobal, cellsPerDim);
            rasterData.maxCellY < cellsPerDim - 1 ? rasterData.maxCellY += 1 : rasterData.maxCellY = cellsPerDim - 1;
            //set dimensions for buffers 
            rasterData.bufferWidth = rasterData.maxCellX - rasterData.minCellX + 1;
            rasterData.bufferHeight = rasterData.maxCellY - rasterData.minCellY + 1;

            return DBERR_OK;
        }

        static uint32_t** calculatePartialAndUncertain(Shape &object, uint32_t &cellsPerDim, RasterData &rasterData){
            //create empty matrix
            uint32_t **M = new uint32_t*[rasterData.bufferWidth]();
            for(uint32_t i=0; i<rasterData.bufferWidth; i++){
                M[i] = new uint32_t[rasterData.bufferHeight]();		
                for(uint32_t j=0; j<rasterData.bufferHeight; j++){
                    M[i][j] = UNCERTAIN;
                }
            }
            // local scope variables
            uint32_t startCellX, startCellY, endCellX, endCellY;;
            uint32_t currentCellX, currentCellY;
            double x1, y1, x2, y2;
            double x, y;
            double ogy1, ogy2;
            int stepX, stepY;
            uint32_t verticalStartY, verticalEndY, horizontalY;
            double tMaxX, tMaxY;
            double tDeltaX, tDeltaY;
            double edgeLength;
            std::deque<bg_point_xy> output;
            double error_margin = 0.00001;
            // get const reference to the shape's vertices
            const std::vector<bg_point_xy>* vertices = object.getReferenceToPoints();
            if (vertices == nullptr) {
                logger::log_error(DBERR_NULL_PTR_EXCEPTION, "Empty vertex list returned for object with id", object.recID);
                return M;
            }

            // logger::log_success("got reference to points");

            // if only one point
            if (object.getVertexCount() == 1) {
                startCellX = (uint32_t)vertices->at(0).x();
                startCellY = (uint32_t)vertices->at(0).y();
                M[startCellX-rasterData.minCellX][startCellY-rasterData.minCellY] = PARTIAL;
                return M;
            }

            // else, loop edges
            for (auto it = vertices->begin(); it != vertices->end()-1; it++) {
                //set points x1 has the lowest x
                if(it->x() < (it+1)->x()){
                    x1 = it->x();
                    y1 = it->y();
                    x2 = (it+1)->x();
                    y2 = (it+1)->y();
                }else{
                    x2 = it->x();
                    y2 = it->y();
                    x1 = (it+1)->x();
                    y1 = (it+1)->y();
                }
                // logger::log_success(x1,y1,x2,y2);

                //keep original values of y1,y2
                ogy1 = y1;
                ogy2 = y2;	

                //set endpoint hilbert cells
                startCellX = (uint32_t)x1;
                startCellY = (uint32_t)y1;
                endCellX = (uint32_t)x2;
                endCellY = (uint32_t)y2;

                if(startCellX == endCellX && startCellY == endCellY){
                    //label the cell in the partially covered matrix
                    M[startCellX-rasterData.minCellX][startCellY-rasterData.minCellY] = PARTIAL;
                }else{
                    //set step (based on direction)
                    stepX = x2 > x1 ? 1 : -1;
                    stepY = y2 > y1 ? 1 : -1;

                    //define the edge
                    bg_linestring ls{{x1, y1},{x2, y2}};
                    edgeLength = boost::geometry::length(ls);

                    //define NEAREST VERTICAL grid line
                    bg_linestring vertical{{(double) (startCellX+1), 0},{(double) (startCellX+1), (double) cellsPerDim}};
                    
                    //define NEAREST HORIZONTAL grid line
                    y1 < y2 ? horizontalY = uint32_t(y1) + 1 : horizontalY = uint32_t(y1);
                    bg_linestring horizontal{{0, (double) horizontalY},{(double) cellsPerDim, (double) horizontalY}};

                    //get intersection points with the vertical and nearest lines
                    boost::geometry::intersection(ls, vertical, output);
                    bg_point_xy intersectionPointVertical = output[0];
                    output.clear();
                    boost::geometry::intersection(ls, horizontal, output);
                    bg_point_xy intersectionPointHorizontal = output[0];
                    output.clear();

                    // //keep in mind: the line segment may not intersect a vertical or horizontal line!!!!!!!!
                    if(boost::geometry::distance(intersectionPointVertical, ls) <= error_margin && boost::geometry::distance(intersectionPointVertical, vertical) <= error_margin){
                        bg_linestring tXMaxline{{x1,y1},intersectionPointVertical};
                        tMaxX = boost::geometry::length(tXMaxline);
                    }else{
                        tMaxX = edgeLength;
                    }
                    if(boost::geometry::distance(intersectionPointHorizontal, ls) <= error_margin && boost::geometry::distance(intersectionPointHorizontal, horizontal) <= error_margin){
                        bg_linestring tYMaxline{{x1,y1},intersectionPointHorizontal};
                        tMaxY = boost::geometry::length(tYMaxline);
                    }else{
                        tMaxY = edgeLength;
                    }

                    //deltas
                    tDeltaX = edgeLength / (x2 - x1);
                    tDeltaY = edgeLength / abs(y2 - y1);
                    
                    //loop (traverse ray)
                    while(startCellX <= x1 && x1 < endCellX+1 && checkY(y1, ogy1, ogy2, startCellY, endCellY)){
                        M[(uint32_t)x1-rasterData.minCellX][(uint32_t)y1-rasterData.minCellY] = PARTIAL;
                        if(tMaxX < tMaxY){
                            x1 = x1 + stepX;
                            tMaxX = tMaxX + tDeltaX;
                        }else{
                            y1 = y1 + stepY;
                            tMaxY = tMaxY + tDeltaY;
                        }
                    }
                }
            }
            return M;
        }
        
        static inline std::vector<uint32_t> getPartialCellsFromMatrix(RasterData &rasterData, uint32_t cellsPerDim, uint32_t **M){
            std::vector<uint32_t> partialCells;
            for(uint32_t i=0; i<rasterData.bufferWidth; i++){
                for(uint32_t j=0; j<rasterData.bufferHeight; j++){
                    if(M[i][j] == PARTIAL){			
                        //store into preallocated array
                        partialCells.emplace_back(hilbert::xy2d(cellsPerDim, i + rasterData.minCellX, j + rasterData.minCellY));
                    }
                }
            }
            return partialCells;
        }

        static DB_STATUS computeAllIntervalsFromPartialCells(uint32_t cellsPerDim, RasterData &rasterData, std::vector<uint32_t> &partialCells, AprilData &aprilData) {
            uint32_t current_id;
            std::vector<uint32_t> allIntervals;
            uint32_t allStart = *partialCells.begin();
            uint32_t currentOrder = allStart;
            for (int i=1; i<partialCells.size(); i++) {
                if (partialCells[i] > currentOrder + 1) {
                    // store interval
                    allIntervals.emplace_back(allStart);
                    allIntervals.emplace_back(currentOrder + 1);
                    allStart = partialCells[i];
                }
                currentOrder = partialCells[i];
            }
            
            // save last interval
            allIntervals.emplace_back(allStart);
            allIntervals.emplace_back(partialCells.back() + 1);

            // store into the aprilData
            aprilData.intervalsALL = allIntervals;
            // aprilData.numIntervalsALL = allIntervals.size() / 2;
            // aprilData.numIntervalsFULL = 0;
            return DBERR_OK;
        }

        static DB_STATUS computeAllAndFullIntervals(Shape &object, uint32_t cellsPerDim, RasterData &rasterData, std::vector<uint32_t> &partialCells, AprilData &aprilData){
            int res;
            bool pip_res;
            uint32_t x,y, current_id;
            std::vector<uint32_t> fullIntervals;
            std::vector<uint32_t> allIntervals;
            //set first partial cell
            auto current_partial_cell = partialCells.begin();
            uint32_t allStart = *current_partial_cell;
            //set first interval start and starting uncertain cell (the next cell after the first partial interval)
            while(*current_partial_cell == *(current_partial_cell+1) - 1){
                current_partial_cell++;
            }
            current_id = *(current_partial_cell) + 1;
            hilbert::d2xy(cellsPerDim, current_id, x, y);
            current_partial_cell++;

            // loop partial cells
            while(current_partial_cell < partialCells.end()){		
                //check neighboring cells
                res = checkNeighborsInInterval(current_id, x, y, rasterData, fullIntervals, partialCells, cellsPerDim);
                if(res == FULL){
                    //no need for pip test, it is full
                    //this full interval ends at the next partial cell
                    fullIntervals.emplace_back(current_id);
                    fullIntervals.emplace_back(*current_partial_cell);
                }else if(res == EMPTY){
                    //current cell is empty
                    //save this all interval
                    allIntervals.emplace_back(allStart);
                    allIntervals.emplace_back(current_id);	
                    //keep next interval's start
                    allStart = *current_partial_cell;		
                }else{
                    //uncertain, must perform PiP test
                    bg_point_xy p(x, y);
                    if(object.pipTest(p)){
                        //current cell is full
                        //this full interval ends at the next partial cell
                        fullIntervals.emplace_back(current_id);
                        fullIntervals.emplace_back(*current_partial_cell);
                    }else{
                        //current cell is empty
                        //save this all interval
                        allIntervals.emplace_back(allStart);
                        allIntervals.emplace_back(current_id);	
                        //keep next interval's start
                        allStart = *current_partial_cell;
                    }
                }

                //get next partial and uncertain
                while(*current_partial_cell == *(current_partial_cell+1) - 1){
                    current_partial_cell++;
                }
                current_id = *(current_partial_cell) + 1;
                hilbert::d2xy(cellsPerDim, current_id, x, y);
                current_partial_cell++;
            }

            // save last interval
            allIntervals.emplace_back(allStart);
            allIntervals.emplace_back(*(current_partial_cell-1) + 1);

            // store into the aprilData
            aprilData.intervalsALL = allIntervals;
            // aprilData.numIntervalsALL = allIntervals.size() / 2;
            aprilData.intervalsFULL = fullIntervals;
            // aprilData.numIntervalsFULL = fullIntervals.size() / 2;
            return DBERR_OK;
        }

        // intervalize shape with area (polygons and rectangles)
        static DB_STATUS intervalizeRegionShape(Shape object, uint32_t cellsPerDim, AprilData &aprilData){
            DB_STATUS ret = DBERR_OK;
            clock_t timer;
            RasterData rasterData;
            //first of all map the object's coordinates to this section's hilbert space
            ret = mapObjectToHilbertSpace(object, rasterData, cellsPerDim);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Couldn't map to hilbert space object with id", object.recID);
                return ret;
            }
            // logger::log_success("mapped");
            // compute partial cells (todo: adjust to return DB_STATUS)
            uint32_t **M = calculatePartialAndUncertain(object, cellsPerDim, rasterData);
            // logger::log_success("partial");
            std::vector<uint32_t> partialCells;
            partialCells = getPartialCellsFromMatrix(rasterData, cellsPerDim, M);
            //sort the cells by cell int 
            sort(partialCells.begin(), partialCells.end());
            // delete the matrix memory, not needed anymore
            for(size_t i = 0; i < rasterData.bufferWidth; i++){
                delete M[i];
            }
            delete M;
            // logger::log_success("sorted");
            // generate all/full intervals
            ret = computeAllAndFullIntervals(object, cellsPerDim, rasterData, partialCells, aprilData);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Computing ALL and FULL intervals failed");
            }
            return ret;
        }

        // intervalize shape without area (points and linestrings)
        static DB_STATUS intervalizeNonRegionShape(Shape object, uint32_t cellsPerDim, AprilData &aprilData){
            DB_STATUS ret = DBERR_OK;
            clock_t timer;
            RasterData rasterData;
            //first of all map the object's coordinates to this section's hilbert space
            ret = mapObjectToHilbertSpace(object, rasterData, cellsPerDim);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Couldn't map to hilbert space object with id", object.recID);
                return ret;
            }
            // compute partial cells
            uint32_t **M = calculatePartialAndUncertain(object, cellsPerDim, rasterData);
            std::vector<uint32_t> partialCells;
            partialCells = getPartialCellsFromMatrix(rasterData, cellsPerDim, M);
            //sort the cells by cell int 
            sort(partialCells.begin(), partialCells.end());
            // delete the matrix memory, not needed anymore
            for(size_t i = 0; i < rasterData.bufferWidth; i++){
                delete M[i];
            }
            delete M;
            // generate all intervals from the partial cells
            ret = computeAllIntervalsFromPartialCells(cellsPerDim, rasterData, partialCells, aprilData);
            return ret;
        }


        namespace disk
        {
            /**
            @brief load and intervalize shapes like polygon or box that have area
             */
            static DB_STATUS loadAndIntervalizeRegionShape(Dataset &dataset, FILE* pFile,  FILE* pFileAPRIL, Shape object) {
                DB_STATUS ret = DBERR_OK;
                int objectsInFullFile = 0;
                // loop objects 
                for(size_t i=0; i<dataset.totalObjects; i++){
                    // reset object
                    object.reset();
                    // get next object to rasterize
                    ret = storage::reader::partitionFile::loadNextObjectComplete(pFile, object);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Failed while reading object number",i,"from partition file");
                        return ret;
                    }
                    // generate APRIL
                    AprilData aprilData;
                    ret = intervalizeRegionShape(object, dataset.aprilConfig.getCellsPerDim(), aprilData);
                    if (ret != DBERR_OK || aprilData.intervalsALL.size() == 0) {
                        // at least 1 ALL interval is needed for each object, otherwise there is an error
                        logger::log_error(DBERR_APRIL_CREATE, "Failed to generate APRIL for object with ID", object.recID);
                        return DBERR_APRIL_CREATE;
                    }
                    // save on disk
                    ret = APRIL::writer::saveAPRIL(pFileAPRIL, object.recID, 0, &aprilData);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Failed while saving APRIL on disk");
                        return ret;
                    }
                }
                return ret;
            }

            /**
            @brief intervalize shapes like point or linestring that have no area
             */
            static DB_STATUS loadAndIntervalizeNonRegionShape(Dataset &dataset, FILE* pFile,  FILE* pFileAPRIL, Shape object) {
                DB_STATUS ret = DBERR_OK;
                // loop objects 
                for(size_t i=0; i<dataset.totalObjects; i++){
                    // reset object
                    object.reset();
                    // get next object to rasterize
                    ret = storage::reader::partitionFile::loadNextObjectComplete(pFile, object);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Failed while reading object number",i,"from partition file");
                        return ret;
                    }
                    // generate APRIL
                    AprilData aprilData;
                    ret = intervalizeNonRegionShape(object, dataset.aprilConfig.getCellsPerDim(), aprilData);
                    if (ret != DBERR_OK || aprilData.intervalsALL.size() == 0) {
                        // at least 1 ALL interval is needed for each object, otherwise there is an error
                        logger::log_error(DBERR_APRIL_CREATE, "Failed to generate APRIL for object with ID", object.recID);
                        return DBERR_APRIL_CREATE;
                    }
                    // save on disk
                    ret = APRIL::writer::saveAPRIL(pFileAPRIL, object.recID, 0, &aprilData);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Failed while saving APRIL on disk");
                        return ret;
                    }
                }
                return ret;
            }

            DB_STATUS init(Dataset &dataset) {
                DB_STATUS ret = DBERR_OK;
                // init rasterization environment
                ret = setRasterBounds(dataset.metadata.dataspaceMetadata);
                if (ret != DBERR_OK) {
                    return DBERR_INVALID_PARAMETER;
                }
                // open dataset file
                FILE* pFile = fopen(dataset.metadata.path.c_str(), "rb");
                if (pFile == NULL) {
                    logger::log_error(DBERR_MISSING_FILE, "Could not open partitioned dataset file from path:", dataset.metadata.path);
                    return DBERR_MISSING_FILE;
                }
                // generate approximation filepaths
                ret = storage::generateAPRILFilePath(&dataset);
                if (ret != DBERR_OK) {
                    return ret;
                }
                // open april files for writing
                FILE* pFileAPRIL = fopen(dataset.aprilConfig.filepath.c_str(), "wb");
                if (pFileAPRIL == NULL) {
                    logger::log_error(DBERR_MISSING_FILE, "Couldnt open APRIL ALL file:", dataset.aprilConfig.filepath);
                    return DBERR_MISSING_FILE;
                }
                // dummy object
                Shape object;
                // next read the dataset metadata
                ret = storage::reader::partitionFile::loadDatasetMetadata(pFile, &dataset);
                if (ret != DBERR_OK) {
                    goto CLOSE_AND_EXIT;
                }
                // write dummy value for object count. 
                fwrite(&dataset.totalObjects, sizeof(size_t), 1, pFileAPRIL);
                // switch based on data type
                switch (dataset.metadata.dataType) {
                    // intervalize dataset objects
                    case DT_POLYGON:
                        object = shape_factory::createEmptyPolygonShape();
                        ret = loadAndIntervalizeRegionShape(dataset, pFile, pFileAPRIL, object);
                        break;
                    case DT_POINT:
                        object = shape_factory::createEmptyPointShape();
                        ret = loadAndIntervalizeNonRegionShape(dataset, pFile, pFileAPRIL, object);
                        break;
                    case DT_LINESTRING:
                        object = shape_factory::createEmptyLineStringShape();
                        ret = loadAndIntervalizeNonRegionShape(dataset, pFile, pFileAPRIL, object);
                        break;
                    case DT_BOX:
                        object = shape_factory::createEmptyRectangleShape();
                        ret = loadAndIntervalizeRegionShape(dataset, pFile, pFileAPRIL, object);
                        break;
                    default:
                        // error
                        logger::log_error(DBERR_INVALID_DATATYPE, "Invalid datatype in intervalization:", dataset.metadata.dataType);
                        return DBERR_INVALID_DATATYPE;
                }
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Failed to load and intervalize dataset contents.");
                    goto CLOSE_AND_EXIT;
                }
        CLOSE_AND_EXIT:
                fclose(pFile);
                fclose(pFileAPRIL);
                return ret;
            }
        }

        namespace memory
        {
            DB_STATUS createAPRILforObject(Shape* shape, DataType dataType, AprilConfig &aprilConfig, AprilData &aprilData) {
                DB_STATUS ret = DBERR_OK;
                // switch based on data type
                switch (dataType) {
                    // intervalize dataset objects
                    case DT_POLYGON:
                    case DT_BOX:
                        ret = intervalizeRegionShape(*shape,  aprilConfig.getCellsPerDim(), aprilData);
                        if (ret != DBERR_OK || aprilData.intervalsALL.size() == 0) {
                            // at least 1 ALL interval is needed for each object, otherwise there is an error
                            logger::log_error(ret, "Failed to generate APRIL for object with ID", shape->recID);
                            return ret;
                        }
                        break;
                    case DT_POINT:
                    case DT_LINESTRING:
                        ret = intervalizeNonRegionShape(*shape,  aprilConfig.getCellsPerDim(), aprilData);
                        if (ret != DBERR_OK || aprilData.intervalsALL.size() == 0) {
                            // at least 1 ALL interval is needed for each object, otherwise there is an error
                            logger::log_error(ret, "Failed to generate APRIL for object with ID", shape->recID);
                            return ret;
                        }
                        break;
                    default:
                        // error
                        logger::log_error(DBERR_INVALID_DATATYPE, "Invalid datatype in intervalization:", dataType);
                        return DBERR_INVALID_DATATYPE;
                }
                return ret;
            }
        }
    }
}