import { useState } from 'react';
import { Card, Form, Input, Button, Typography, Alert, App } from 'antd';
import { UserOutlined, LockOutlined } from '@ant-design/icons';
import { useNavigate, useSearchParams, Link } from 'react-router-dom';
import { login as loginApi, getCurrentUser } from '@/api/auth';
import { useAuthStore } from '@/store/authStore';
import { connectWebSocket } from '@/lib/websocket';

const { Title, Text } = Typography;

interface LoginFormValues {
  username: string;
  password: string;
}

/**
 * Login page: POST /api/users/login -> store JWT -> redirect.
 */
export default function LoginPage() {
  const [form] = Form.useForm<LoginFormValues>();
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const { message } = App.useApp();
  const setToken = useAuthStore((s) => s.setToken);
  const setUser = useAuthStore((s) => s.setUser);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = async (values: LoginFormValues) => {
    setLoading(true);
    setError(null);
    try {
      const res = await loginApi(values);
      setToken(res.token);

      // Fetch full profile to enrich the auth store.
      try {
        const user = await getCurrentUser();
        setUser(user);
      } catch {
        // Profile fetch is best-effort; JWT claims are enough for basic UI.
      }

      // Connect WebSocket with the new token.
      connectWebSocket().catch((e) => console.warn('WS connect failed:', e));

      const redirect = searchParams.get('redirect') || '/';
      message.success('Login successful!');
      navigate(redirect, { replace: true });
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : 'Login failed';
      setError(msg);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div
      style={{
        minHeight: '100vh',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        background: '#f0f2f5',
      }}
    >
      <Card style={{ width: 400, boxShadow: '0 4px 12px rgba(0,0,0,0.1)' }}>
        <div style={{ textAlign: 'center', marginBottom: 24 }}>
          <Title level={2} style={{ marginBottom: 4 }}>
            Welcome Back
          </Title>
          <Text type="secondary">Sign in to your auction account</Text>
        </div>

        {error && (
          <Alert
            type="error"
            message={error}
            showIcon
            closable
            onClose={() => setError(null)}
            style={{ marginBottom: 16 }}
          />
        )}

        <Form
          form={form}
          name="login"
          onFinish={handleSubmit}
          autoComplete="off"
          size="large"
        >
          <Form.Item
            name="username"
            rules={[{ required: true, message: 'Please input your username!' }]}
          >
            <Input prefix={<UserOutlined />} placeholder="Username" />
          </Form.Item>

          <Form.Item
            name="password"
            rules={[{ required: true, message: 'Please input your password!' }]}
          >
            <Input.Password prefix={<LockOutlined />} placeholder="Password" />
          </Form.Item>

          <Form.Item>
            <Button type="primary" htmlType="submit" loading={loading} block>
              Log in
            </Button>
          </Form.Item>
        </Form>

        <div style={{ textAlign: 'center' }}>
          <Text type="secondary">
            Don't have an account? <Link to="/register">Register now</Link>
          </Text>
        </div>
      </Card>
    </div>
  );
}