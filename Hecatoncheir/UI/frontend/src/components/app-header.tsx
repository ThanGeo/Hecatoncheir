// src/components/AppHeader.tsx
import React from 'react';
import { Typography, Tooltip } from 'antd';
import { InfoCircleOutlined } from '@ant-design/icons';

const { Title } = Typography;

const AppHeader: React.FC = () => {
  const hecatoncheirInfo = (
    <div>
      <p>
        Hecatoncheir is a prototype distributed big spatial data management tool
        built with MPI that offers lightning-fast spatial query processing.
      </p>
      <p>
        Inspired by Hecatoncheires, the sons of Gaia (mother earth). With their
        hundreds of hands, they multitask to scale spatial data management up to
        infinity.
      </p>
    </div>
  );

  return (
    <div style={{
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      borderBottom: '1px solid #e8e8e8',
      // backgroundColor:"white"
      // marginBottom: '2%'
    }}>
      <Tooltip title={hecatoncheirInfo} placement="right">
        <img
          src="logo-transparent.png"
          alt="Hecatoncheir Logo"
          style={{
            width: '15vh',
            height: '15vh',
            backgroundColor: "transparent",
            cursor: 'pointer'
          }}
        />
      </Tooltip>
      <Title level={2} style={{ margin: 0, marginRight: 20 }}>
        Hecatoncheir
      </Title>
    </div>
  );
};

export default AppHeader;