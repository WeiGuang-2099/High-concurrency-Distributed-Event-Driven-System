import { Layout } from 'antd';
import { Outlet } from 'react-router-dom';
import Header from './Header';

const { Content } = Layout;

/**
 * Main layout with header + content area (Outlet for nested routes).
 */
export function MainLayout() {
  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Header />
      <Content style={{ background: '#f0f2f5', minHeight: 'calc(100vh - 64px)' }}>
        <Outlet />
      </Content>
    </Layout>
  );
}

export default MainLayout;