import React, { useState, useEffect } from 'react';
import { Form, Select, Upload, Button, message, Space, Progress } from 'antd';
import { UploadOutlined, ReloadOutlined } from '@ant-design/icons';
import type { UploadProps } from 'antd/es/upload/interface';
import Input from 'antd/es/input/Input';

const { Option } = Select;

type QueryType = 'rangeQuery' | 'knnQuery' | 'spatialJoins' | null;
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
        !allFields.datasetFile || allFields.datasetFile.length === 0 ||
        !allFields.queryDatasetFile || allFields.queryDatasetFile.length === 0 ||
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
        !allFields.leftDatasetFile || allFields.leftDatasetFile.length === 0 ||
        !allFields.rightDatasetFile || allFields.rightDatasetFile.length === 0 ||
        !allFields.predicate ||
        !allFields.spatialDataType ||
        !allFields.querySetType
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
          return prevProgress; // stay at 99% until actual completion
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
      'datasetFile',
      'queryDatasetFile',
      'leftDatasetFile',
      'rightDatasetFile',
      'kValue',
    ]);
    setTimeout(checkFormValidity, 0);
  };

  const commonUploadProps: UploadProps = {
    accept: '.tsv,.csv,.wkt',
    beforeUpload: (file) => {
      const isTsc = file.name.endsWith('.tsv');
      const isCsv = file.name.endsWith('.csv');
      const isWkt = file.name.endsWith('.wkt');
      const isAllowed = isTsc || isCsv || isWkt;
      if (!isAllowed) {
        message.error(`${file.name} is not a .tsv, .wkt or .csv file!`);
      }
      return isAllowed || Upload.LIST_IGNORE;
    },
    maxCount: 1,
    onRemove: () => {
      return true;
    },
  };

  const handleFinish = async (values: any) => {
    setIsLoading(true);
    message.loading('Processing query...', 0);

    const formData = new FormData();

    formData.append('queryType', values.queryType || '');

    if (values.queryType === 'rangeQuery' || values.queryType === 'knnQuery') {
      if (values.spatialDataType) formData.append('spatialDataType', values.spatialDataType);
      if (values.querySetType) formData.append('querySetType', values.querySetType);

      if (values.datasetFile && values.datasetFile.length > 0 && values.datasetFile[0].originFileObj) {
        formData.append('datasetFile', values.datasetFile[0].originFileObj);
      }
      if (values.queryDatasetFile && values.queryDatasetFile.length > 0 && values.queryDatasetFile[0].originFileObj) {
        formData.append('queryDatasetFile', values.queryDatasetFile[0].originFileObj);
      }

      if (values.queryType === 'knnQuery' && values.kValue) {
        formData.append('kValue', values.kValue);
      }
    } else if (values.queryType === 'spatialJoins') {
      if (values.predicate) formData.append('predicate', values.predicate);
      if (values.spatialDataType) formData.append('spatialDataType', values.spatialDataType);
      if (values.querySetType) formData.append('querySetType', values.querySetType);

      if (values.leftDatasetFile && values.leftDatasetFile.length > 0 && values.leftDatasetFile[0].originFileObj) {
        formData.append('leftDatasetFile', values.leftDatasetFile[0].originFileObj);
      }
      if (values.rightDatasetFile && values.rightDatasetFile.length > 0 && values.rightDatasetFile[0].originFileObj) {
        formData.append('rightDatasetFile', values.rightDatasetFile[0].originFileObj);
      }
    }

    try {
      const response = await fetch('http://localhost:5000/process-query', {
        method: 'POST',
        body: formData,
      });

      // Complete the progress bar
      setLoadingProgress(100);
      await new Promise(resolve => setTimeout(resolve, 300)); // Brief pause at 100%

      message.destroy();
      setIsLoading(false);

      if (response.ok) {
        const result = await response.json();
        message.success('Query submitted successfully!');
        console.log('Backend response:', result);
      } else {
        const errorText = await response.text();
        message.error(`Query failed: ${errorText}`);
        console.error('Backend error:', response.status, errorText);
      }
    } catch (error) {
      setLoadingProgress(100);
      await new Promise(resolve => setTimeout(resolve, 300));
      
      message.destroy();
      setIsLoading(false);
      message.error('Network error or server unavailable.');
      console.error('Fetch error:', error);
    }
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
      } else {
        const errorText = await response.text();
        console.error("Clear error:", errorText);
      }
    } catch (err) {
      console.error("Network error while clearing:", err);
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
    setIsLoading(true);
    message.loading('Preparing dataset...', 0);
  
    const formData = new FormData();
    formData.append('queryType', values.queryType || '');
  
    if (values.queryType === 'rangeQuery' || values.queryType === 'knnQuery') {
      if (values.spatialDataType) formData.append('spatialDataType', values.spatialDataType);
      if (values.querySetType) formData.append('querySetType', values.querySetType);
  
      if (values.datasetFile?.[0]?.originFileObj) {
        formData.append('datasetFile', values.datasetFile[0].originFileObj);
      }
      if (values.queryDatasetFile?.[0]?.originFileObj) {
        formData.append('queryDatasetFile', values.queryDatasetFile[0].originFileObj);
      }
      if (values.queryType === 'knnQuery' && values.kValue) {
        formData.append('kValue', values.kValue);
      }
    } else if (values.queryType === 'spatialJoins') {
      if (values.predicate) formData.append('predicate', values.predicate);
      if (values.spatialDataType) formData.append('spatialDataType', values.spatialDataType);
      if (values.querySetType) formData.append('querySetType', values.querySetType);
  
      if (values.leftDatasetFile?.[0]?.originFileObj) {
        formData.append('leftDatasetFile', values.leftDatasetFile[0].originFileObj);
      }
      if (values.rightDatasetFile?.[0]?.originFileObj) {
        formData.append('rightDatasetFile', values.rightDatasetFile[0].originFileObj);
      }
    }
  
    try {
      const response = await fetch('http://localhost:5000/prepare-hec', {
        method: 'POST',
        body: formData,
      });
  
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
        const errorText = await response.text();
        message.error(`Prepare failed: ${errorText}`);
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
      await new Promise(r => setTimeout(r, 300));
  
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
      await new Promise(r => setTimeout(r, 300));
      
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
      onFinish={handleFinish}
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
        </Select>
      </Form.Item>

      {queryType && (
        <>
          {(queryType === 'rangeQuery' || queryType === 'knnQuery') && (
            <>
              <Space direction="vertical" style={{ width: '100%' }}>
                <div style={{ display: 'flex', gap: '16px', flexWrap: 'wrap' }}>
                  <Form.Item
                    name="datasetFile"
                    label="Dataset File"
                    valuePropName="fileList"
                    getValueFromEvent={(e) => e && e.fileList}
                    rules={[{ required: true, message: 'Please select a dataset file!' }]}
                    style={{ flex: 1, minWidth: '250px' }}
                  >
                    <Upload {...commonUploadProps}  style={{ width:"-moz-available"}}>
                      <Button icon={<UploadOutlined />} style={{ width: '100%' }}>Select Dataset File (.tsv, .wkt or .csv)</Button>
                    </Upload>
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
                    name="queryDatasetFile"
                    label="Query Dataset File"
                    valuePropName="fileList"
                    getValueFromEvent={(e) => e && e.fileList}
                    rules={[{ required: true, message: 'Please select a query dataset file!' }]}
                    style={{ flex: 1, minWidth: '250px' }}
                  >
                    <Upload {...commonUploadProps}  style={{ width:"-moz-available"}}>
                      <Button icon={<UploadOutlined />} style={{ width: '100%' }}>Select Query Dataset File (.tsv, .wkt or .csv)</Button>
                    </Upload>
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
                    rules={[{ required: true, message: 'Please input K value!', min: 1 }]}
                    style={{ maxWidth: '250px' }}
                  >
                    <Input type="number" placeholder="Enter K value" min="1"/>
                  </Form.Item>
                )}
              </Space>
            </>
          )}

          {queryType === 'spatialJoins' && (
            <>
              <Space direction="vertical" style={{ width: '100%' }}>
                <div style={{ display: 'flex', gap: '16px', flexWrap: 'wrap' }}>
                  <Form.Item
                    name="leftDatasetFile"
                    label="Left Dataset File"
                    valuePropName="fileList"
                    getValueFromEvent={(e) => e && e.fileList}
                    rules={[{ required: true, message: 'Please select a left dataset file!' }]}
                    style={{ flex: 1, minWidth: '250px' }}
                  >
                    <Upload {...commonUploadProps}  style={{ width:"-moz-available"}}>
                      <Button icon={<UploadOutlined />} style={{ width: '100%' }}>Select Left Dataset File (.tsv, .wkt or .csv)</Button>
                    </Upload>
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
                    name="rightDatasetFile"
                    label="Right Dataset File"
                    valuePropName="fileList"
                    getValueFromEvent={(e) => e && e.fileList}
                    rules={[{ required: true, message: 'Please select a right dataset file!' }]}
                    style={{ flex: 1, minWidth: '250px' }}
                  >
                    <Upload {...commonUploadProps}  style={{ width:"-moz-available"}}>
                      <Button icon={<UploadOutlined />} style={{ width: '100%' }}>Select Right Dataset File (.tsv, .wkt or .csv)</Button>
                    </Upload>
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

      { queryType == null && (
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