import { useEffect } from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { ConfigProvider, App as AntApp } from 'antd';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

import { useAuthStore } from '@/store/authStore';
import { connectWebSocket } from '@/lib/websocket';

import MainLayout from '@/components/layout/MainLayout';
import RequireAuth from '@/components/auth/RequireAuth';

import HomePage from '@/pages/HomePage';
import LoginPage from '@/pages/LoginPage';
import RegisterPage from '@/pages/RegisterPage';
import AuctionDetailPage from '@/pages/AuctionDetailPage';
import TicketPurchasePage from '@/pages/TicketPurchasePage';
import OrderPayPage from '@/pages/OrderPayPage';
import ProfilePage from '@/pages/ProfilePage';
import NotFoundPage from '@/pages/NotFoundPage';

import AdminLayout from '@/pages/admin/AdminLayout';
import DashboardPage from '@/pages/admin/DashboardPage';
import CreateAuctionPage from '@/pages/admin/CreateAuctionPage';
import CreateTicketPage from '@/pages/admin/CreateTicketPage';

// TanStack Query client with sensible defaults.
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false,
      staleTime: 30_000,
    },
  },
});

/**
 * Root application component.
 *
 * On mount: hydrate auth state from localStorage and connect WebSocket
 * if a valid token is present.
 */
function App() {
  const hydrate = useAuthStore((s) => s.hydrate);
  const isAuthenticated = useAuthStore((s) => s.isAuthenticated);

  useEffect(() => {
    hydrate();
  }, [hydrate]);

  // Connect WebSocket when authenticated (and disconnect handled on logout).
  useEffect(() => {
    if (isAuthenticated) {
      connectWebSocket().catch((e) => console.warn('Initial WS connect failed:', e));
    }
  }, [isAuthenticated]);

  return (
    <QueryClientProvider client={queryClient}>
      <ConfigProvider
        theme={{
          token: {
            colorPrimary: '#1890ff',
            borderRadius: 6,
          },
        }}
      >
        <AntApp>
          <BrowserRouter>
            <Routes>
              {/* Public routes */}
              <Route path="/login" element={<LoginPage />} />
              <Route path="/register" element={<RegisterPage />} />

              {/* Protected routes with main layout */}
              <Route
                element={
                  <MainLayout />
                }
              >
                <Route path="/" element={<HomePage />} />
                <Route path="/auctions/:id" element={<AuctionDetailPage />} />
                <Route
                  path="/tickets/:eventId"
                  element={
                    <RequireAuth>
                      <TicketPurchasePage />
                    </RequireAuth>
                  }
                />
                <Route
                  path="/orders/:id/pay"
                  element={
                    <RequireAuth>
                      <OrderPayPage />
                    </RequireAuth>
                  }
                />
                <Route
                  path="/profile"
                  element={
                    <RequireAuth>
                      <ProfilePage />
                    </RequireAuth>
                  }
                />
              </Route>

              {/* Admin routes */}
              <Route
                path="/admin"
                element={
                  <RequireAuth adminOnly>
                    <AdminLayout />
                  </RequireAuth>
                }
              >
                <Route index element={<DashboardPage />} />
                <Route path="auctions/create" element={<CreateAuctionPage />} />
                <Route path="tickets/create" element={<CreateTicketPage />} />
              </Route>

              {/* Fallback */}
              <Route path="/404" element={<NotFoundPage />} />
              <Route path="*" element={<Navigate to="/404" replace />} />
            </Routes>
          </BrowserRouter>
        </AntApp>
      </ConfigProvider>
    </QueryClientProvider>
  );
}

export default App;