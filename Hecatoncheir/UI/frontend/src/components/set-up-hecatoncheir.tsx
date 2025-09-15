import React, { useEffect } from 'react';
import { Form, Button, message } from 'antd';
import TextArea from 'antd/es/input/TextArea';

interface PC {
  nameOrIp: string;
}

interface ClusterSetupProps {
  onClusterInitialized: () => void;
  pcNamesInput: string;
  setPcNamesInput: (value: string) => void;
  currentPcs: PC[];
}

const ClusterSetup: React.FC<ClusterSetupProps> = ({
  onClusterInitialized,
  pcNamesInput,
  setPcNamesInput,
  currentPcs,
}) => {
  const [form] = Form.useForm();

  useEffect(() => {
    form.setFieldsValue({ pcNames: pcNamesInput });
  }, [pcNamesInput, form]);

  const handleTextAreaChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    setPcNamesInput(e.target.value.replace(" ",""));
  };

  const handleInitHec = async (values: { pcNames: string }) => {
    message.loading('Initializing HEC cluster...', 0);

    const names = values.pcNames
      .split('\n')
      .map((name) => name.trim())
      .filter((name) => name !== '');

    const pcsToSend = names.map((name) => ({ nameOrIp: name }));

    console.log('PCs to send to backend:', pcsToSend);

    const clusterConfig = {
      numPcs: pcsToSend.length,
      pcs: pcsToSend,
    };

    try {
      const response = await fetch('http://localhost:5000/init-hec', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(clusterConfig),
      });

      message.destroy();

      if (response.ok) {
        const result = await response.json();
        message.success('HEC cluster initialized successfully!');
        console.log('Backend response (init-hec):', result);
        onClusterInitialized();
      } else {
        const errorText = await response.text();
        message.error(`HEC initialization failed: ${errorText}`);
        console.error('Backend error (init-hec):', response.status, errorText);
      }
    } catch (error) {
      message.destroy();
      message.error('Network error or server unavailable during HEC initialization.');
      console.error('Fetch error (init-hec):', error);
    }
  };

  return (
    <Form
      form={form}
      layout="vertical"
      initialValues={{ pcNames: pcNamesInput }}
      onFinish={handleInitHec}
      style={{ padding: '20px', border: '1px solid #eee', borderRadius: '8px', marginTop: '2.5%' }}
    >
      <Form.Item
        label="Node aliases/IPs in the Cluster (one per line - change line via enter button in your keyboard)"
        name="pcNames"
        rules={[
          {
            required: true,
            message: 'Please enter at least one PC name or IP.',
            validator: (_, value) => {
              const names = (value || '')
                .split('\n')
                .map((name: string) => name.trim())
                .filter((name: string) => name !== '');
              if (names.length === 0) {
                return Promise.reject('Please enter at least one PC name or IP.');
              }
              return Promise.resolve();
            },
          },
        ]}
      >
        <TextArea
          placeholder="Enter the aliases or IPs of the nodes, one per line"
          style={{ width: '100%', minHeight: '120px', maxHeight:'60vh' }}
          value={pcNamesInput}
          onChange={handleTextAreaChange}
        />
      </Form.Item>

      <Form.Item style={{ marginTop: 20 }}>
        <Button
          type="primary"
          htmlType="submit"
          size="large"
          style={{ width: '100%' }}
          disabled={pcNamesInput.trim().length === 0}
        >
          Init Hecatoncheir
        </Button>
      </Form.Item>
    </Form>
  );
};

export default ClusterSetup;