const express = require('express');
const cors = require('cors');
const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');

const app = express();
const port = 5000;

app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));

app.use((req, res, next) => {
    console.log(`\n=== ${req.method} ${req.path} ===`);
    console.log('Headers:', req.headers);
    console.log('Body:', req.body);
    next();
});

const projectRoot = path.resolve(__dirname, '..', '..', '..');
const cppServerPath = path.join(projectRoot, 'build', 'Hecatoncheir', 'UI', 'hec_server');

const mpiCmd = "mpirun.mpich";
const mpiArgs = ["-np", "1", cppServerPath];

let cppServer = null;
let cppPending = false;

function getFileFormat(filename) {
    const ext = path.extname(filename).toLowerCase();
    if (ext === '.csv') return 'CSV';
    if (ext === '.tsv') return 'TSV';
    if (ext === '.wkt') return 'WKT';
    return 'WKT';
}

function mapGeometryType(frontendType) {
    const mapping = {
        'points': 'POINT',
        'point': 'POINT',
        'line': 'LINE',
        'polygon': 'POLYGON',
        'box': 'BOX' 
    };
    return mapping[frontendType] || 'POINT';
}

function validateFilePath(filePath) {
    console.log('Validating file path:', filePath);
    
    if (!filePath) {
        return { valid: false, error: 'File path is required' };
    }
    
    const absolutePath = path.isAbsolute(filePath) ? filePath : path.resolve(filePath);
    console.log('Absolute path:', absolutePath);
    
    if (!fs.existsSync(absolutePath)) {
        return { valid: false, error: `File not found: ${absolutePath}` };
    }
    
    const stats = fs.statSync(absolutePath);
    if (!stats.isFile()) {
        return { valid: false, error: `Path is not a file: ${absolutePath}` };
    }
    
    const ext = path.extname(absolutePath).toLowerCase();
    if (!['.csv', '.tsv', '.wkt'].includes(ext)) {
        return { valid: false, error: `Invalid file type. Must be .csv, .tsv, or .wkt` };
    }
    
    return { valid: true, absolutePath };
}

function startCppServer() {
    console.log('Starting C++ server...');
    cppServer = spawn(mpiCmd, mpiArgs);

    cppServer.stdout.on('data', (data) => {
        console.log(`C++: ${data.toString()}`);
    });

    cppServer.stderr.on('data', (data) => {
        console.error(`C++ ERR: ${data.toString()}`);
    });

    cppServer.on('close', (code) => {
        console.log(`C++ server exited with code ${code}`);
        cppServer = null;
    });
}

function sendCommandToCpp(cmd, { timeoutMs = 120000 } = {}) {
    return new Promise((resolve, reject) => {
        if (cppPending) {
            return reject(new Error('Another command is already pending'));
        }
        cppPending = true;

        let buffer = '';
        let timeout = null;

        const cleanup = () => {
            cppServer.stdout.off('data', onData);
            if (timeout) clearTimeout(timeout);
            cppPending = false;
        };

        timeout = setTimeout(() => {
            cleanup();
            reject(new Error('Timeout waiting for response from C++'));
        }, timeoutMs);

        const tryExtractJSON = () => {
            let s = buffer;

            while (true) {
                const firstBrace = s.indexOf('{');
                const lastBrace = s.lastIndexOf('}');

                if (firstBrace === -1 || lastBrace === -1 || lastBrace <= firstBrace) {
                    break;
                }

                const candidate = s.slice(firstBrace, lastBrace + 1).trim();

                try {
                    const parsed = JSON.parse(candidate);
                    buffer = s.slice(lastBrace + 1);
                    return parsed;
                } catch (err) {
                    const prevLast = s.lastIndexOf('}', lastBrace - 1);
                    if (prevLast === -1 || prevLast <= firstBrace) {
                        break;
                    } else {
                        s = s.slice(0, prevLast + 1);
                        continue;
                    }
                }
            }

            return null;
        };

        const onData = (data) => {
            buffer += data.toString();
            buffer = buffer.replace(/\r/g, '');
            
            let parsed;
            try {
                parsed = tryExtractJSON();
            } catch (err) {
                cleanup();
                return reject(err);
            }

            if (parsed !== null) {
                cleanup();
                return resolve(parsed);
            }
        };

        cppServer.stdout.on('data', onData);

        try {
            console.log('Sending to C++:', JSON.stringify(cmd));
            cppServer.stdin.write(JSON.stringify(cmd) + '\n');
        } catch (err) {
            cleanup();
            return reject(err);
        }
    });
}

app.post('/init-hec', async (req, res) => {
    console.log('INIT-HEC called');
    try {
        if (!cppServer) {
            console.log("Spawning fresh C++ process...");
            startCppServer();
            // Give it a moment to start
            await new Promise(resolve => setTimeout(resolve, 1000));
        }
        const hosts = req.body.pcs.map(pc => pc.nameOrIp + ":1");
        console.log('Initializing with hosts:', hosts);
        const response = await sendCommandToCpp({ action: "init", hosts });
        console.log('Init response:', response);
        res.json(response);
    } catch (err) {
        console.error('Init error:', err);
        res.status(500).json({ status: 'error', message: err.message });
    }
});


app.post('/prepare-hec', async (req, res) => {
  console.log('\n========== PREPARE-HEC CALLED ==========');
  console.log('Request body:', JSON.stringify(req.body, null, 2));
  
  const { queryType, kValue, predicate, spatialDataType, querySetType, distance } = req.body;

  try {
      if (!queryType) {
          console.log('ERROR: No query type provided');
          return res.status(400).json({ status: 'error', message: 'Query type is required' });
      }

      console.log('Query type:', queryType);

      const requestBody = { action: "prepare", queryType };

      if (queryType === 'knnQuery' || queryType === 'rangeQuery') {
          const { datasetPath, queryDatasetPath } = req.body;
          
          console.log('Dataset path received:', datasetPath);
          console.log('Query dataset path received:', queryDatasetPath);
          
          const datasetValidation = validateFilePath(datasetPath);
          if (!datasetValidation.valid) {
              console.log('Dataset validation failed:', datasetValidation.error);
              return res.status(400).json({ status: 'error', message: `Dataset: ${datasetValidation.error}` });
          }
          
          const queryDatasetValidation = validateFilePath(queryDatasetPath);
          if (!queryDatasetValidation.valid) {
              console.log('Query dataset validation failed:', queryDatasetValidation.error);
              return res.status(400).json({ status: 'error', message: `Query Dataset: ${queryDatasetValidation.error}` });
          }

          requestBody.dataset = datasetValidation.absolutePath;
          requestBody.queryDataset = queryDatasetValidation.absolutePath;
          requestBody.fileFormat = getFileFormat(datasetValidation.absolutePath);
          requestBody.queryFileFormat = getFileFormat(queryDatasetValidation.absolutePath);
          requestBody.geometryType = mapGeometryType(spatialDataType);
          requestBody.queryGeometryType = mapGeometryType(querySetType);
          
          if (kValue) requestBody.kValue = parseInt(kValue);

          console.log('Prepared request body:', requestBody);

      } else if (queryType === 'distanceJoins') {
          const { datasetPath, queryDatasetPath } = req.body;
          
          console.log('Dataset path received:', datasetPath);
          console.log('Query dataset path received:', queryDatasetPath);
          console.log('Distance threshold:', distance);
          
          const datasetValidation = validateFilePath(datasetPath);
          if (!datasetValidation.valid) {
              console.log('Dataset validation failed:', datasetValidation.error);
              return res.status(400).json({ status: 'error', message: `Dataset: ${datasetValidation.error}` });
          }
          
          const queryDatasetValidation = validateFilePath(queryDatasetPath);
          if (!queryDatasetValidation.valid) {
              console.log('Query dataset validation failed:', queryDatasetValidation.error);
              return res.status(400).json({ status: 'error', message: `Query Dataset: ${queryDatasetValidation.error}` });
          }

          requestBody.dataset = datasetValidation.absolutePath;
          requestBody.queryDataset = queryDatasetValidation.absolutePath;
          requestBody.fileFormat = getFileFormat(datasetValidation.absolutePath);
          requestBody.queryFileFormat = getFileFormat(queryDatasetValidation.absolutePath);
          requestBody.distance = parseFloat(distance);

          console.log('Prepared request body for distance joins:', requestBody);

      } else if (queryType === 'spatialJoins') {
          // FIX: Use leftDatasetPath and rightDatasetPath for spatial joins
          const { leftDatasetPath, rightDatasetPath } = req.body;
          
          console.log('Left dataset path received:', leftDatasetPath);
          console.log('Right dataset path received:', rightDatasetPath);
          console.log('Predicate:', predicate);
          
          const leftValidation = validateFilePath(leftDatasetPath);
          if (!leftValidation.valid) {
              console.log('Left dataset validation failed:', leftValidation.error);
              return res.status(400).json({ status: 'error', message: `Left Dataset: ${leftValidation.error}` });
          }
          
          const rightValidation = validateFilePath(rightDatasetPath);
          if (!rightValidation.valid) {
              console.log('Right dataset validation failed:', rightValidation.error);
              return res.status(400).json({ status: 'error', message: `Right Dataset: ${rightValidation.error}` });
          }

          // FIX: Map to dataset and queryDataset for C++ backend
          requestBody.dataset = leftValidation.absolutePath;
          requestBody.queryDataset = rightValidation.absolutePath;
          requestBody.fileFormat = getFileFormat(leftValidation.absolutePath);
          requestBody.queryFileFormat = getFileFormat(rightValidation.absolutePath);
          requestBody.geometryType = mapGeometryType(spatialDataType);
          requestBody.queryGeometryType = mapGeometryType(querySetType);
          
          if (predicate) {
              requestBody.predicate = predicate;
          }

          console.log('Prepared request body for spatial joins:', requestBody);
      }

      console.log('Sending to C++:', JSON.stringify(requestBody, null, 2));
      const response = await sendCommandToCpp(requestBody, { timeoutMs: 300000 });
      console.log('C++ response:', response);
      res.json(response);

  } catch (err) {
      console.error('Prepare error:', err);
      res.status(500).json({ status: 'error', message: err.message });
  }
});

app.post('/execute-hec', async (req, res) => {
    console.log('EXECUTE-HEC called');
    try {
        const response = await sendCommandToCpp({ action: "execute" }, { timeoutMs: 300000 });
        console.log('Execute response:', response);
        res.json(response);
    } catch (err) {
        console.error('Execute error:', err);
        res.status(500).json({ status: 'error', message: err.message });
    }
});

app.post('/clear', async (req, res) => {
    console.log('CLEAR called');
    try {
        const response = await sendCommandToCpp({ action: "clear" });
        console.log('Clear response:', response);
        res.json(response);
    } catch (err) {
        console.error('Clear error:', err);
        res.status(500).json({ status: 'error', message: err.message });
    }
});

app.get('/download-results/:filename', (req, res) => {
    console.log('DOWNLOAD-RESULTS called for:', req.params.filename);
    const filename = req.params.filename;
    const filepath = path.join(projectRoot, 'build', 'Hecatoncheir', 'UI', filename);
    
    if (fs.existsSync(filepath)) {
        res.download(filepath, filename, (err) => {
            if (err) {
                console.error('Download error:', err);
            }
            fs.unlinkSync(filepath);
        });
    } else {
        res.status(404).json({ error: 'File not found' });
    }
});

app.post('/terminate-hec', async (req, res) => {
    console.log('TERMINATE-HEC called');
    try {
        const response = await sendCommandToCpp({ action: "terminate" });
        console.log('Terminate response:', response);
        res.json(response);
    } catch (err) {
        console.error('Terminate error:', err);
        res.status(500).json({ status: 'error', message: err.message });
    }
});

// Test endpoint
app.get('/test', (req, res) => {
    res.json({ status: 'Server is running', timestamp: new Date().toISOString() });
});

app.listen(port, () => {
    console.log(`\n====================================`);
    console.log(`Node.js server listening on http://localhost:${port}`);
    console.log(`Test the server: curl http://localhost:${port}/test`);
    console.log(`====================================\n`);
});