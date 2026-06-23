import { type ReactNode } from 'react';
import { Navigate, useLocation } from 'react-router-dom';
import { useAuthStore } from '@/store/authStore';

interface RequireAuthProps {
  children: ReactNode;
  /** If true, only allow admin users. */
  adminOnly?: boolean;
}

/**
 * Route guard: redirects unauthenticated users to /login.
 * When `adminOnly` is set, non-admin users are redirected to /.
 */
export function RequireAuth({ children, adminOnly = false }: RequireAuthProps) {
  const location = useLocation();
  const isAuthenticated = useAuthStore((s) => s.isAuthenticated);
  const isAdmin = useAuthStore((s) => s.isAdmin);

  if (!isAuthenticated) {
    const redirect = encodeURIComponent(location.pathname + location.search);
    return <Navigate to={`/login?redirect=${redirect}`} replace />;
  }

  if (adminOnly && !isAdmin) {
    return <Navigate to="/" replace />;
  }

  return <>{children}</>;
}

export default RequireAuth;