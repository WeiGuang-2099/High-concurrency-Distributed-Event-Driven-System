import axios, { AxiosError, AxiosResponse, InternalAxiosRequestConfig } from 'axios';
import { message } from 'antd';
import type { ApiResponse, ErrorResponse } from '@/types';

/**
 * localStorage key for the JWT access token.
 * The value is set/cleared by the auth store (Zustand).
 */
export const TOKEN_STORAGE_KEY = 'auction_token';

/**
 * Path that should not trigger a 401 redirect (to avoid loops on the login page).
 */
const PUBLIC_PATHS = ['/api/users/login', '/api/users/register'];

const baseURL = import.meta.env.VITE_API_BASE_URL || '';

export const apiClient = axios.create({
  baseURL,
  timeout: 15000,
  headers: {
    'Content-Type': 'application/json',
  },
});

/* -------------------------------------------------------------------------- */
/* Request interceptor: attach JWT                                           */
/* -------------------------------------------------------------------------- */
apiClient.interceptors.request.use(
  (config: InternalAxiosRequestConfig) => {
    const token = localStorage.getItem(TOKEN_STORAGE_KEY);
    if (token && config.headers) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  },
  (error: unknown) => Promise.reject(error),
);

/* -------------------------------------------------------------------------- */
/* Response interceptor: unwrap ApiResponse + centralized error handling     */
/* -------------------------------------------------------------------------- */
apiClient.interceptors.response.use(
  (response: AxiosResponse<ApiResponse<unknown>>) => {
    const body = response.data;

    // Successful business response: unwrap `data` field.
    if (body && typeof body.code === 'number' && body.code >= 200 && body.code < 300) {
      // Return the unwrapped payload so callers can use it directly.
      // We patch `data` to be the inner data, keeping axios response shape.
      response.data = body.data as never;
      return response;
    }

    // Business-level error returned with HTTP 200 but non-success code.
    const errorMsg = body?.message || 'Request failed';
    message.error(errorMsg);
    return Promise.reject(new Error(errorMsg));
  },
  (error: AxiosError<ErrorResponse>) => {
    const status = error.response?.status;
    const requestUrl = error.config?.url || '';
    const isPublicPath = PUBLIC_PATHS.some((p) => requestUrl.includes(p));

    // Handle 401 Unauthorized: clear token and redirect to login.
    if (status === 401 && !isPublicPath) {
      localStorage.removeItem(TOKEN_STORAGE_KEY);
      // Avoid redirect loop if already on login/register page.
      const currentPath = window.location.pathname;
      if (!currentPath.startsWith('/login') && !currentPath.startsWith('/register')) {
        message.warning('Session expired, please log in again.');
        window.location.href = `/login?redirect=${encodeURIComponent(currentPath)}`;
      }
      return Promise.reject(error);
    }

    // Extract error message from known shapes.
    let errorMsg = 'Network error, please try again';
    if (error.response?.data) {
      errorMsg = error.response.data.message || errorMsg;
    } else if (error.message) {
      errorMsg = error.message;
    }

    message.error(errorMsg);
    return Promise.reject(error);
  },
);

/**
 * Typed helper for GET requests that returns the unwrapped data directly.
 */
export async function get<T>(url: string, params?: Record<string, unknown>): Promise<T> {
  const res = await apiClient.get<T>(url, { params });
  return res.data;
}

/**
 * Typed helper for POST requests that returns the unwrapped data directly.
 */
export async function post<T>(url: string, body?: unknown): Promise<T> {
  const res = await apiClient.post<T>(url, body);
  return res.data;
}

/**
 * Typed helper for PUT requests that returns the unwrapped data directly.
 */
export async function put<T>(url: string, body?: unknown): Promise<T> {
  const res = await apiClient.put<T>(url, body);
  return res.data;
}

/**
 * Typed helper for DELETE requests that returns the unwrapped data directly.
 */
export async function del<T>(url: string): Promise<T> {
  const res = await apiClient.delete<T>(url);
  return res.data;
}