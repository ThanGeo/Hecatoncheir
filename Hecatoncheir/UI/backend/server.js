const express = require('express');
const multer = require('multer');
const cors = require('cors');
const { exec } = require('child_process');
const path = require('path');
const fs = require('fs');

const app = express();
const port = 5000;

app.use(cors());
app.use(express.json());

const UPLOADS_DIR = path.join(__dirname, 'uploads');
const TEMP_FILES_DIR = path.join(__dirname, 'temp_files');
const UNIFIED_EXECUTABLE_PATH = path.join(__dirname, 'build', 'main');

if (!fs.existsSync(UPLOADS_DIR)) {
    fs.mkdirSync(UPLOADS_DIR);
}
if (!fs.existsSync(TEMP_FILES_DIR)) {
    fs.mkdirSync(TEMP_FILES_DIR);
}

const upload = multer({ dest: UPLOADS_DIR });

// Helper functions (same as before)
const saveUploadedFile = (fileObject, tempFilePathsToCleanup) => {
    if (!fileObject) return null;
    const tempFileName = `${Date.now()}-${Math.random().toString(36).substring(2, 15)}-${fileObject.originalname}`;
    const tempPath = path.join(TEMP_FILES_DIR, tempFileName);

    try {
        fs.renameSync(fileObject.path, tempPath);
        tempFilePathsToCleanup.push(tempPath);
        return tempPath;
    } catch (error) {
        return null;
    }
};

const cleanupTempFiles = (tempFilePathsToCleanup, multerFiles) => {
    // ... same implementation as before
};

// Main query processing endpoint
app.post('/process-query', upload.fields([
    { name: 'datasetFile', maxCount: 1 },
    { name: 'queryDatasetFile', maxCount: 1 },
    { name: 'leftDatasetFile', maxCount: 1 },
    { name: 'rightDatasetFile', maxCount: 1 },
]), (req, res) => {
    const { queryType, spatialDataType, querySetType, kValue, predicate } = req.body;
    const files = req.files;

    console.log('Received form data:', { queryType, spatialDataType, querySetType, kValue, predicate });

    const datasetFile = files.datasetFile ? files.datasetFile[0] : null;
    const queryDatasetFile = files.queryDatasetFile ? files.queryDatasetFile[0] : null;
    const leftDatasetFile = files.leftDatasetFile ? files.leftDatasetFile[0] : null;
    const rightDatasetFile = files.rightDatasetFile ? files.rightDatasetFile[0] : null;

    let cppArgs = ['prepare', '--queryType', queryType];
    const tempFilePathsToCleanup = [];
    const allMulterTempFiles = [datasetFile, queryDatasetFile, leftDatasetFile, rightDatasetFile].filter(Boolean);

    try {
        if (!queryType) {
            cleanupTempFiles(tempFilePathsToCleanup, allMulterTempFiles);
            return res.status(400).send('Query type is required.');
        }

        if (queryType === 'rangeQuery' || queryType === 'knnQuery') {
            const dsPath = saveUploadedFile(datasetFile, tempFilePathsToCleanup);
            const queryDsPath = saveUploadedFile(queryDatasetFile, tempFilePathsToCleanup);
            
            if (!dsPath || !queryDsPath) {
                cleanupTempFiles(tempFilePathsToCleanup, allMulterTempFiles);
                return res.status(400).send('Dataset and Query Dataset files are required.');
            }
            
            cppArgs.push('--dataset', dsPath);
            cppArgs.push('--queryDataset', queryDsPath);
            
            if (spatialDataType) cppArgs.push('--spatialDataType', spatialDataType);
            if (querySetType) cppArgs.push('--querySetType', querySetType);
            if (queryType === 'knnQuery' && kValue) cppArgs.push('--kValue', kValue);
            
        } else if (queryType === 'spatialJoins') {
            const leftDsPath = saveUploadedFile(leftDatasetFile, tempFilePathsToCleanup);
            const rightDsPath = saveUploadedFile(rightDatasetFile, tempFilePathsToCleanup);
            
            if (!leftDsPath || !rightDsPath) {
                cleanupTempFiles(tempFilePathsToCleanup, allMulterTempFiles);
                return res.status(400).send('Left and Right Dataset files are required for Spatial Joins.');
            }
            
            cppArgs.push('--leftDataset', leftDsPath);
            cppArgs.push('--rightDataset', rightDsPath);
            if (predicate) cppArgs.push('--predicate', predicate);
        }

        console.log(`Executing: ${UNIFIED_EXECUTABLE_PATH} ${cppArgs.join(' ')}`);

        exec(`"${UNIFIED_EXECUTABLE_PATH}" ${cppArgs.map(arg => `"${arg}"`).join(' ')}`, (error, stdout, stderr) => {
            cleanupTempFiles(tempFilePathsToCleanup, allMulterTempFiles);

            if (error) {
                console.error('Execution error:', error);
                return res.status(500).json({
                    message: 'Error executing program',
                    error: error.message,
                    details: stderr
                });
            }

            res.json({
                message: 'Program executed successfully',
                output: stdout,
                stderr: stderr
            });
        });

    } catch (err) {
        console.error('Server error:', err);
        cleanupTempFiles(tempFilePathsToCleanup, allMulterTempFiles);
        res.status(500).send('Internal server error.');
    }
});

// HEC management endpoints
app.post('/init-hec', (req, res) => {
    const { numPcs, pcs } = req.body;

    if (!Array.isArray(pcs)) {
        return res.status(400).send('Invalid data for HEC initialization.');
    }

    let cppArgs = ['init'];
    pcs.forEach((pc) => {
        if (pc.nameOrIp) cppArgs.push(pc.nameOrIp);
    });

    console.log(`Executing HEC init: ${UNIFIED_EXECUTABLE_PATH} ${cppArgs.join(' ')}`);

    exec(`"${UNIFIED_EXECUTABLE_PATH}" ${cppArgs.map(arg => `"${arg}"`).join(' ')}`, (error, stdout, stderr) => {
        if (error) {
            console.error('HEC init error:', error);
            return res.status(500).json({
                message: 'Error initializing HEC',
                error: error.message,
                details: stderr
            });
        }

        res.json({
            message: 'HEC initialized successfully',
            output: stdout,
            stderr: stderr
        });
    });
});

app.post('/execute-hec', (req, res) => {
    console.log('Executing HEC operations...');

    exec(`"${UNIFIED_EXECUTABLE_PATH}" execute`, (error, stdout, stderr) => {
        if (error) {
            console.error('HEC execute error:', error);
            return res.status(500).json({
                message: 'Error executing HEC operations',
                error: error.message,
                details: stderr
            });
        }

        res.json({
            message: 'HEC operations executed successfully',
            output: stdout,
            stderr: stderr
        });
    });
});

app.post('/terminate-hec', (req, res) => {
    console.log('Terminating HEC...');

    exec(`"${UNIFIED_EXECUTABLE_PATH}" terminate`, (error, stdout, stderr) => {
        if (error) {
            console.error('HEC terminate error:', error);
            return res.status(500).json({
                message: 'Error terminating HEC',
                error: error.message,
                details: stderr
            });
        }

        res.json({
            message: 'HEC terminated successfully',
            output: stdout,
            stderr: stderr
        });
    });
});

app.listen(port, () => {
    console.log(`Backend server listening at http://localhost:${port}`);
    console.log(`Expecting executable at: ${UNIFIED_EXECUTABLE_PATH}`);
});