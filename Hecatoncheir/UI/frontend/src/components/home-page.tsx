import React, { useState, useEffect } from 'react';
import { Form, Select, Button, message, Space, Progress, Input } from 'antd';
import { ReloadOutlined, FolderOpenOutlined } from '@ant-design/icons';

const { Option } = Select;

type QueryType = 'rangeQuery' | 'knnQuery' | 'spatialJoins' | 'distanceJoins' | null;
type SpatialDataType = 'polygon' | 'points' | 'line' | null;
type RangeQuerySetType = 'polygon' | 'box' | null;
type KNNQuerySetType = 'point' | 'polygon' | null;
type PredicateType = 'intersect' | 'inside' | 'contains' | 'covers' | 'coveredBy' | 'equals' | 'meet' | 'findRelation' | null;
type SpatialJoinQuerySetType = 'polygon' | 'box' | 'point' | null;

interface HomePageProps {
  onTerminateHec: () => void;
}

const HomePage: React.FC<HomePageProps> = ({ onTerminateHec }) => {
  const [form] = Form.useForm();
  const [queryType, setQueryType] = useState<QueryType>(null);
  const [spatialDataType, setSpatialDataType] = useState<SpatialDataType>(null);
  const [querySetType, setQuerySetType] = useState<RangeQuerySetType | KNNQuerySetType | SpatialJoinQuerySetType | null>(null);
  const [predicate, setPredicate] = useState<PredicateType>(null);
  const [isPrepareDatasetDisabled, setIsPrepareDatasetDisabled] = useState(true);
  const [isPrepared, setIsPrepared] = useState(false);
  const [resultsData, setResultsData] = useState<any>(null);

  // States for the loading bar
  const [loadingProgress, setLoadingProgress] = useState(0);
  const [isLoading, setIsLoading] = useState(false);

  const checkFormValidity = () => {
    const allFields = form.getFieldsValue();
    let allRequiredFilled = true;

    if (!allFields.queryType) {
      allRequiredFilled = false;
    }

    if (allFields.queryType === 'rangeQuery' || allFields.queryType === 'knnQuery') {
      if (
        !allFields.datasetPath ||
        !allFields.queryDatasetPath ||
        !allFields.spatialDataType ||
        !allFields.querySetType
      ) {
        allRequiredFilled = false;
      }
      if (allFields.queryType === 'knnQuery' && !allFields.kValue) {
        allRequiredFilled = false;
      }
    } else if (allFields.queryType === 'spatialJoins') {
      if (
        !allFields.leftDatasetPath ||
        !allFields.rightDatasetPath ||
        !allFields.predicate ||
        !allFields.spatialDataType ||
        !allFields.querySetType
      ) {
        allRequiredFilled = false;
      }
    } else if (allFields.queryType === 'distanceJoins') {
      if (
        !allFields.datasetPath ||
        !allFields.queryDatasetPath ||
        !allFields.distance
      ) {
        allRequiredFilled = false;
      }
    }

    setIsPrepareDatasetDisabled(!allRequiredFilled);
  };

  useEffect(() => {
    checkFormValidity();
  }, [queryType, spatialDataType, querySetType, predicate]);

  useEffect(() => {
    let interval: NodeJS.Timeout | undefined = undefined;
    if (isLoading) {
      setLoadingProgress(0);
      interval = setInterval(() => {
        setLoadingProgress((prevProgress) => {
          if (prevProgress < 70) {
            return prevProgress + 2;
          } else if (prevProgress < 90) {
            return prevProgress + 0.5;
          } else if (prevProgress < 99) {
            return prevProgress + 0.1;
          }
          return prevProgress;
        });
      }, 200);
    } else {
      if (interval) {
        clearInterval(interval);
      }
      setLoadingProgress(0);
    }

    return () => {
      if (interval) {
        clearInterval(interval);
      }
    };
  }, [isLoading]);

  const handleQueryTypeChange = (value: QueryType) => {
    setQueryType(value);
    setSpatialDataType(null);
    setQuerySetType(null);
    setPredicate(null);
    setIsPrepared(false);
    form.resetFields([
      'spatialDataType',
      'querySetType',
      'predicate',
      'datasetPath',
      'queryDatasetPath',
      'leftDatasetPath',
      'rightDatasetPath',
      'kValue',
      'distance',
    ]);
    setTimeout(checkFormValidity, 0);
  };

  const handleResetForm = () => {
    form.resetFields();
    setQueryType(null);
    setSpatialDataType(null);
    setQuerySetType(null);
    setPredicate(null);
    setIsPrepared(false);
    setResultsData(null);
    setTimeout(checkFormValidity, 0);
  };

  const handleClearButtonClick = async () => {
    try {
      const response = await fetch('http://localhost:5000/clear', {
        method: 'POST',
      });
  
      if (response.ok) {
        const result = await response.json();
        console.log("Clear response:", result);
        handleResetForm();
        setResultsData(null);
        message.success('Form cleared successfully!');
      } else {
        const errorText = await response.text();
        console.error("Clear error:", errorText);
        message.error(`Clear failed: ${errorText}`);
      }
    } catch (err) {
      console.error("Network error while clearing:", err);
      message.error('Network error while clearing.');
    }
  };

  const handleTerminateButtonClick = async () => { 
    try {
      const response = await fetch('http://localhost:5000/terminate-hec', {
        method: 'POST',
      });
  
      if (response.ok) {
        const result = await response.json();
        console.log("Terminate-hec response:", result);
        message.success("HEC terminated successfully!");
        onTerminateHec();
      } else {
        const errorText = await response.text();
        console.error("Terminate error:", errorText);
        message.error(`Failed to terminate HEC: ${errorText}`);
      }
    } catch (err) {
      console.error("Network error while terminating:", err);
      message.error("Network error while terminating HEC.");
    }
  };

  const handlePrepareDataset = async () => {
    const values = form.getFieldsValue();
    console.log('Form values:', values);
    
    setIsLoading(true);
    message.loading('Preparing dataset...', 0);
  
    const requestBody: any = {
      queryType: values.queryType || '',
    };
  
    if (values.queryType === 'rangeQuery' || values.queryType === 'knnQuery') {
      requestBody.spatialDataType = values.spatialDataType;
      requestBody.querySetType = values.querySetType;
      requestBody.datasetPath = values.datasetPath;
      requestBody.queryDatasetPath = values.queryDatasetPath;
  
      if (values.queryType === 'knnQuery' && values.kValue) {
        requestBody.kValue = values.kValue;
      }
    } else if (values.queryType === 'spatialJoins') {
      requestBody.predicate = values.predicate;
      requestBody.spatialDataType = values.spatialDataType;
      requestBody.querySetType = values.querySetType;
      requestBody.leftDatasetPath = values.leftDatasetPath;
      requestBody.rightDatasetPath = values.rightDatasetPath;
    } else if (values.queryType === 'distanceJoins') {
      requestBody.datasetPath = values.datasetPath;
      requestBody.queryDatasetPath = values.queryDatasetPath;
      requestBody.distance = parseFloat(values.distance);
    }
    
    console.log('Sending request body:', requestBody);
  
    try {
      const response = await fetch('http://localhost:5000/prepare-hec', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(requestBody),
      });
      
      console.log('Response status:', response.status);
  
      setLoadingProgress(100);
      await new Promise(r => setTimeout(r, 300));
  
      message.destroy();
      setIsLoading(false);
  
      if (response.ok) {
        const result = await response.json();
        message.success('Dataset prepared successfully!');
        console.log('Prepare-hec response:', result);
        setIsPrepared(true);
      } else {
        const errorData = await response.json();
        message.error(`Prepare failed: ${errorData.message || 'Unknown error'}`);
      }
    } catch (err) {
      setLoadingProgress(100);
      await new Promise(r => setTimeout(r, 300));
      
      message.destroy();
      setIsLoading(false);
      message.error('Network error while preparing dataset.');
    }
  };
  
  const handleExecuteQuery = async () => {
    setIsLoading(true);
    message.loading('Executing query...', 0);
  
    try {
      const response = await fetch('http://localhost:5000/execute-hec', {
        method: 'POST',
      });
  
      setLoadingProgress(100);
      
      // Wait longer to ensure UI renders the 100% state
      await new Promise(r => setTimeout(r, 600));
  
      message.destroy();
      setIsLoading(false);
  
      if (response.ok) {
        const result = await response.json();
        message.success('Query executed successfully!');
        console.log('Execute-hec response:', result);
  
        if (result.resultsData) {
          setResultsData(result.resultsData);
        }
      } else {
        const errorText = await response.text();
        message.error(`Execution failed: ${errorText}`);
      }
    } catch (err) {
      setLoadingProgress(100);
      await new Promise(r => setTimeout(r, 600));
      
      message.destroy();
      setIsLoading(false);
      message.error('Network error while executing query.');
    }
  };
  const handleDownloadResults = () => {
    if (!resultsData) {
      message.error('No results available to download.');
      return;
    }
  
    try {
      const jsonString = JSON.stringify(resultsData, null, 4);
      const blob = new Blob([jsonString], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      
      const a = document.createElement('a');
      a.href = url;
      a.download = `query_results_${resultsData.timestamp || Date.now()}.json`;
      document.body.appendChild(a);
      a.click();
      
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
      
      message.success('Results downloaded successfully!');
    } catch (error) {
      console.error('Download error:', error);
      message.error('Failed to download results.');
    }
  };

  return (
    <Form
      form={form}
      layout="vertical"
      initialValues={{ queryType: null }}
      style={{ maxWidth: "100%", margin: '20px auto', padding: '25px', border: '1px solid #eee', borderRadius: '8px' }}
      onValuesChange={checkFormValidity}
    >
      <Form.Item
        name="queryType"
        label="Query Type"
        rules={[{ required: true, message: 'Please select a query type!'}]}
      >
        <Select placeholder="Select a query type" onChange={handleQueryTypeChange}>
          <Option value="rangeQuery">Range Query</Option>
          <Option value="knnQuery">KNN Query</Option>
          <Option value="spatialJoins">Spatial Join</Option>
          <Option value="distanceJoins">Distance Join</Option>
        </Select>
      </Form.Item>

      {queryType && (
        <>
          {(queryType === 'rangeQuery' || queryType === 'knnQuery') && (
            <>
              <Space direction="vertical" style={{ width: '100%' }}>
                <div style={{ display: 'flex', gap: '16px', flexWrap: 'wrap' }}>
                  <Form.Item
                    name="datasetPath"
                    label="Dataset File Path"
                    rules={[{ required: true, message: 'Please enter dataset file path!' }]}
                    style={{ flex: 1, minWidth: '250px' }}
                  >
                    <Input 
                      placeholder="/path/to/dataset.csv" 
                      prefix={<FolderOpenOutlined />}
                    />
                  </Form.Item>
                  <Form.Item
                    name="spatialDataType"
                    label="Spatial Data Type"
                    rules={[{ required: true, message: 'Please select a spatial data type!' }]}
                    style={{ flex: 1, minWidth: '250px' }}
                  >
                    <Select placeholder="Select spatial data type" onChange={(value: SpatialDataType) => setSpatialDataType(value)}>
                      <Option value="polygon">Polygon</Option>
                      <Option value="points">Points</Option>
                      <Option value="line">Line</Option>
                    </Select>
                  </Form.Item>
                </div>

                <div style={{ display: 'flex', gap: '16px', flexWrap: 'wrap' }}>
                  <Form.Item
                    name="queryDatasetPath"
                    label="Query Dataset File Path"
                    rules={[{ required: true, message: 'Please enter query dataset file path!' }]}
                    style={{ flex: 1, minWidth: '250px' }}
                  >
                    <Input 
                      placeholder="/path/to/queries.csv" 
                      prefix={<FolderOpenOutlined />}
                    />
                  </Form.Item>
                  <Form.Item
                    name="querySetType"
                    label="Query Set Type"
                    rules={[{ required: true, message: 'Please select a query set type!' }]}
                    style={{ flex: 1, minWidth: '250px' }}
                  >
                    <Select placeholder="Select query set type" onChange={(value: RangeQuerySetType | KNNQuerySetType) => setQuerySetType(value)}>
                      {queryType === 'rangeQuery' && (
                        <>
                          <Option value="polygon">Polygon</Option>
                          <Option value="box">Box</Option>
                        </>
                      )}
                      {queryType === 'knnQuery' && (
                        <>
                          <Option value="point">Point</Option>
                          <Option value="polygon">Polygon</Option>
                        </>
                      )}
                    </Select>
                  </Form.Item>
                </div>

                {queryType === 'knnQuery' && (
                  <Form.Item
                    name="kValue"
                    label="K Value (for KNN)"
                    rules={[{ required: true, message: 'Please input K value!' }]}
                    style={{ maxWidth: '250px' }}
                  >
                    <Input type="number" placeholder="Enter K value" min={1}/>
                  </Form.Item>
                )}
              </Space>
            </>
          )}

          {queryType === 'distanceJoins' && (
            <>
              <Space direction="vertical" style={{ width: '100%' }}>
                <div style={{ display: 'flex', gap: '16px', flexWrap: 'wrap' }}>
                  <Form.Item
                    name="datasetPath"
                    label="Dataset File Path"
                    rules={[{ required: true, message: 'Please enter dataset file path!' }]}
                    style={{ flex: 1, minWidth: '250px' }}
                  >
                    <Input 
                      placeholder="/path/to/dataset.csv" 
                      prefix={<FolderOpenOutlined />}
                    />
                  </Form.Item>
                </div>

                <div style={{ display: 'flex', gap: '16px', flexWrap: 'wrap' }}>
                  <Form.Item
                    name="queryDatasetPath"
                    label="Query Dataset File Path"
                    rules={[{ required: true, message: 'Please enter query dataset file path!' }]}
                    style={{ flex: 1, minWidth: '250px' }}
                  >
                    <Input 
                      placeholder="/path/to/queries.csv" 
                      prefix={<FolderOpenOutlined />}
                    />
                  </Form.Item>

                  <Form.Item
                    name="distance"
                    label="Distance Threshold"
                    rules={[{ required: true, message: 'Please input distance threshold!' }]}
                    style={{ flex: 1, minWidth: '250px' }}
                  >
                    <Input type="number" placeholder="Enter distance (float)" step={0.001} min={0}/>
                  </Form.Item>
                </div>
              </Space>
            </>
          )}

          {queryType === 'spatialJoins' && (
            <>
              <Space direction="vertical" style={{ width: '100%' }}>
                <div style={{ display: 'flex', gap: '16px', flexWrap: 'wrap' }}>
                  <Form.Item
                    name="leftDatasetPath"
                    label="Left Dataset File Path"
                    rules={[{ required: true, message: 'Please enter left dataset file path!' }]}
                    style={{ flex: 1, minWidth: '250px' }}
                  >
                    <Input 
                      placeholder="/path/to/left_dataset.csv" 
                      prefix={<FolderOpenOutlined />}
                    />
                  </Form.Item>

                  <Form.Item
                    name="spatialDataType"
                    label="Left Dataset Type"
                    rules={[{ required: true, message: 'Please select a spatial data type!' }]}
                    style={{ flex: 1, minWidth: '250px' }}
                  >
                    <Select placeholder="Select spatial data type" onChange={(value: SpatialDataType) => setSpatialDataType(value)}>
                      <Option value="points">Points</Option>
                      <Option value="line">Line</Option>
                      <Option value="polygon">Polygon</Option>
                    </Select>
                  </Form.Item>
                </div>

                <div style={{ display: 'flex', gap: '16px', flexWrap: 'wrap' }}>
                  <Form.Item
                    name="rightDatasetPath"
                    label="Right Dataset File Path"
                    rules={[{ required: true, message: 'Please enter right dataset file path!' }]}
                    style={{ flex: 1, minWidth: '250px' }}
                  >
                    <Input 
                      placeholder="/path/to/right_dataset.csv" 
                      prefix={<FolderOpenOutlined />}
                    />
                  </Form.Item>

                  <Form.Item
                    name="querySetType"
                    label="Right Dataset Type"
                    rules={[{ required: true, message: 'Please select a query set type!' }]}
                    style={{ flex: 1, minWidth: '250px' }}
                  >
                    <Select placeholder="Select query set type" onChange={(value: SpatialJoinQuerySetType) => setQuerySetType(value)}>
                      <Option value="points">Points</Option>
                      <Option value="line">Line</Option>
                      <Option value="polygon">Polygon</Option>
                    </Select>
                  </Form.Item>
                </div>

                <Form.Item
                  name="predicate"
                  label="Predicate"
                  rules={[{ required: true, message: 'Please select a predicate!' }]}
                  style={{ maxWidth: '250px' }}
                >
                  <Select placeholder="Select predicate" onChange={(value: PredicateType) => setPredicate(value)}>
                    <Option value="intersect">Intersect</Option>
                    <Option value="inside">Inside</Option>
                    <Option value="contains">Contains</Option>
                    <Option value="covers">Covers</Option>
                    <Option value="coveredBy">Covered By</Option>
                    <Option value="equals">Equals</Option>
                    <Option value="meet">Meet</Option>
                    <Option value="findRelation">Find Relation</Option>
                  </Select>
                </Form.Item>
              </Space>
            </>
          )}

          <Form.Item style={{textAlignLast:"right"}}>
            <Space>
              <Button type="default" danger onClick={handleTerminateButtonClick} disabled={isLoading}>
                Terminate Hecatoncheir
              </Button>
              <Button type="default" icon={<ReloadOutlined />} onClick={handleClearButtonClick} disabled={isLoading}>
                Clear Form
              </Button>
              <Button type="primary" onClick={handlePrepareDataset} disabled={isPrepareDatasetDisabled || isLoading}>
                Prepare Dataset
              </Button>
              <Button type="primary" onClick={handleExecuteQuery} disabled={!isPrepared || isLoading}>
                Submit Query
              </Button>
              <Button 
                type="default" 
                onClick={handleDownloadResults} 
                disabled={!resultsData || isLoading}
              >
                Download Results
              </Button>
            </Space>
          </Form.Item>
        </>
      )}

      {queryType == null && (
        <>
          <Form.Item style={{ display: 'flex', justifyContent: 'flex-end', marginTop: '20px' }}>
            <Button type="default" danger onClick={handleTerminateButtonClick} disabled={isLoading}>
              Terminate Hecatoncheir
            </Button>
          </Form.Item>
        </>
      )}

      {/* Loading bar, directly below the form */}
      {isLoading && (
        <div style={{ marginTop: '20px' }}>
          <Progress percent={loadingProgress} status="active" showInfo={true} format={(percent) => `${percent?.toFixed(1)}%`} />
          <p style={{ textAlign: 'center', marginTop: '10px', color: '#888', fontFamily: 'monospace', letterSpacing: '1px', }}>Processing query....</p>
        </div>
      )}
    </Form>
  );
};

export default HomePage;