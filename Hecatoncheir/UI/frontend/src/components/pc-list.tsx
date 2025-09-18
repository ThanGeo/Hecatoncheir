// src/components/PCListDisplay.tsx
import React from 'react';
import { Collapse, Typography, Space, Input, Divider } from 'antd';
import { MinusCircleOutlined, PlusOutlined } from '@ant-design/icons'; // Import icons if you want to customize

const { Panel } = Collapse;
const { Text } = Typography;

interface PC {
  nameOrIp: string;
}

interface PCListDisplayProps {
  pcs: PC[];
}

const PCListDisplay: React.FC<PCListDisplayProps> = ({ pcs }) => {

  return (
    <Collapse
      bordered={false}
      defaultActiveKey={['1']}
      expandIconPosition="end"
      style={{
        backgroundColor: '#f0f2f5',
        borderRadius: '8px',
        marginBottom: '20px',
        marginTop:"6.5%"
      }}
    >
      <Panel
        header={<Text strong>Nodes ({pcs.length})</Text>}
        key="1"
        style={{ borderBottom: 'none' }}
      >
        <div style={{ maxHeight: '49.5vh', overflowY: 'auto' }}>
          {pcs.map((pc, index) => (
            <Space key={index} style={{ display: 'flex', marginBottom: 8 }} align="baseline">
              <Text style={{ minWidth: '60px' }}>Node {index + 1}:</Text>
              <Input value={pc.nameOrIp} readOnly style={{ flexGrow: 1 }} disabled={true} /> <span style={{color:"green"}}></span>
            </Space>
          ))}
        </div>
        <Divider/>
        {/* {pcs.length > 0 && (
          <span style={{color:"green", marginLeft:"25%"}}>All nodes are running</span>
        )} */}
      </Panel>
    </Collapse>
  );
};

export default PCListDisplay;