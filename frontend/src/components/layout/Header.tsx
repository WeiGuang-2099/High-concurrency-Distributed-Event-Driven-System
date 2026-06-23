import { Layout, Menu, Dropdown, Button, Space, Typography, Avatar } from 'antd';
import { UserOutlined, LogoutOutlined, HomeOutlined, SettingOutlined } from '@ant-design/icons';
import { Link, useNavigate } from 'react-router-dom';
import { useAuthStore } from '@/store/authStore';
import { logout as logoutApi } from '@/api/auth';
import { wsManager } from '@/lib/websocket';
import NotificationBell from '@/components/notification/NotificationBell';

const { Header: AntHeader } = Layout;
const { Text } = Typography;

/**
 * Global top navigation bar.
 * Shows logo, navigation links, notification bell, and user menu.
 */
export function Header() {
  const navigate = useNavigate();
  const isAuthenticated = useAuthStore((s) => s.isAuthenticated);
  const isAdmin = useAuthStore((s) => s.isAdmin);
  const user = useAuthStore((s) => s.user);
  const clear = useAuthStore((s) => s.clear);

  const handleLogout = async () => {
    try {
      await logoutApi();
    } catch {
      // ignore network errors on logout
    }
    wsManager.deactivate();
    clear();
    navigate('/login');
  };

  const userMenuItems = [
    {
      key: 'profile',
      icon: <UserOutlined />,
      label: 'My Profile',
      onClick: () => navigate('/profile'),
    },
    ...(isAdmin
      ? [
          {
            key: 'admin',
            icon: <SettingOutlined />,
            label: 'Admin Panel',
            onClick: () => navigate('/admin'),
          },
        ]
      : []),
    { type: 'divider' as const },
    {
      key: 'logout',
      icon: <LogoutOutlined />,
      label: 'Logout',
      onClick: handleLogout,
    },
  ];

  return (
    <AntHeader
      style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        background: '#fff',
        padding: '0 24px',
        borderBottom: '1px solid #f0f0f0',
        boxShadow: '0 1px 4px rgba(0,21,41,0.08)',
      }}
    >
      {/* Left: Logo + nav */}
      <Space size="large">
        <Link to="/" style={{ fontSize: 20, fontWeight: 700, color: '#1890ff' }}>
          AuctionHub
        </Link>
        <Menu mode="horizontal" selectedKeys={[]} style={{ borderBottom: 'none' }}>
          <Menu.Item key="home" icon={<HomeOutlined />}>
            <Link to="/">Home</Link>
          </Menu.Item>
        </Menu>
      </Space>

      {/* Right: auth + notifications */}
      <Space size="middle">
        {isAuthenticated && <NotificationBell />}

        {isAuthenticated ? (
          <Dropdown menu={{ items: userMenuItems }} placement="bottomRight">
            <Space style={{ cursor: 'pointer' }}>
              <Avatar icon={<UserOutlined />} style={{ backgroundColor: '#1890ff' }} />
              <Text>{user?.username || 'User'}</Text>
            </Space>
          </Dropdown>
        ) : (
          <Space>
            <Button type="link" onClick={() => navigate('/login')}>
              Login
            </Button>
            <Button type="primary" onClick={() => navigate('/register')}>
              Register
            </Button>
          </Space>
        )}
      </Space>
    </AntHeader>
  );
}

export default Header;