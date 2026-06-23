/**
 * Auth-related types.
 * Mirrors: com.auction.user.controller.dto.*
 */

export interface LoginRequest {
  username: string;
  password: string;
}

export interface LoginResponse {
  token: string;
  tokenType: string;
  expiresIn: number;
}

export interface RegisterRequest {
  username: string;
  email: string;
  password: string;
}

export interface UpdateProfileRequest {
  email: string;
}

export interface User {
  id: number;
  username: string;
  email: string;
  role: string;
  createdAt: string;
}

/**
 * Decoded JWT payload structure.
 * Claims used by the backend gateway filter:
 *   sub=userId, username, roles, jti, iss, exp
 */
export interface JwtPayload {
  sub: string;
  username: string;
  roles: string;
  jti: string;
  iss: string;
  exp: number;
  iat?: number;
}