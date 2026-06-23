import { get, post } from './client';
import type { LoginRequest, LoginResponse, RegisterRequest, User } from '@/types';

/**
 * POST /api/users/login
 */
export function login(payload: LoginRequest): Promise<LoginResponse> {
  return post<LoginResponse>('/api/users/login', payload);
}

/**
 * POST /api/users/register
 */
export function register(payload: RegisterRequest): Promise<User> {
  return post<User>('/api/users/register', payload);
}

/**
 * GET /api/users/me
 */
export function getCurrentUser(): Promise<User> {
  return get<User>('/api/users/me');
}

/**
 * POST /api/users/logout
 */
export function logout(): Promise<void> {
  return post<void>('/api/users/logout');
}