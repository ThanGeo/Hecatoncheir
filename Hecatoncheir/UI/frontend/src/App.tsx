// src/App.tsx
import React, { useState, useEffect } from 'react';
import './App.css';
import AppHeader from './components/app-header';
import ClusterSetup from './components/set-up-hecatoncheir';
import HomePage from './components/home-page';
import PCListDisplay from './components/pc-list';

import { Layout, Row, Col, Typography, theme } from 'antd';

const { Content, Footer } = Layout;
const { Title } = Typography;

function App() {
  const [isClusterInitialized, setIsClusterInitialized] = useState(false);
  const [pcNamesInput, setPcNamesInput] = useState<string>('');
  const [currentPcs, setCurrentPcs] = useState<{ nameOrIp: string }[]>([]);

  const {
    token: { colorBgContainer, borderRadiusLG },
  } = theme.useToken();

  useEffect(() => {
    const names = pcNamesInput
      .split('\n')
      .map((name) => name.trim())
      .filter((name) => name !== '');
    setCurrentPcs(names.map((name) => ({ nameOrIp: name })));
  }, [pcNamesInput]);

  const handleClusterInitialization = () => {
    console.log("Cluster initialized, switching to HomePage...");
    setIsClusterInitialized(true);
  };

  const handleTerminateHec = () => {
    setIsClusterInitialized(false);
    setPcNamesInput('');
    setCurrentPcs([]);
  };

  return (
    <>
    <div style={{width:"99.45%"}}>
      <AppHeader/>
      <Content style={{ marginTop: '2%'}}>
        <div
          style={{
            // minHeight: 'calc(100vh - 114px)',
            // background: colorBgContainer,
            borderRadius: borderRadiusLG,
          }}
        >
          <Row gutter={17}>
            <Col span={5} style={{marginLeft:"20%", marginTop:"0.5%"}}>
              <PCListDisplay pcs={currentPcs} />
            </Col>
            <Col span={10}>
              {isClusterInitialized ? (
                <>
                <div style={{width:"100%"}}>
                  <span style={{ color: 'black', marginLeft:"45%"}}>STATUS:</span> <span style={{ color: 'green' }}>Active</span>
                  <HomePage onTerminateHec={handleTerminateHec} />
                </div>
                </>
              ) : (
                <>
                  <span style={{ color: 'black', marginLeft:"45%", marginBottom:"2px"}}>STATUS:</span> <span style={{ color: 'red' }}>Inactive</span>
                  <ClusterSetup
                    onClusterInitialized={handleClusterInitialization}
                    pcNamesInput={pcNamesInput}
                    setPcNamesInput={setPcNamesInput}
                    currentPcs={currentPcs}
                  />
                </>
              )}
            </Col>
          </Row>
        </div>
      </Content>
      </div>
    </>
  );
}

export default App;