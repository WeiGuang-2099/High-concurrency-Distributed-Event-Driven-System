import type { JwtPayload } from '@/types';

/**
 * Decode a JWT payload without verifying the signature.
 * Signature verification is performed server-side (gateway + WS interceptor),
 * so the frontend only needs the claims for UI decisions (e.g. role checks).
 *
 * @param token Raw JWT string (with or without "Bearer " prefix)
 * @returns Decoded payload, or null if the token is malformed.
 */
export function decodeJwt(token: string): JwtPayload | null {
  try {
    const raw = token.startsWith('Bearer ') ? token.slice(7) : token;
    const parts = raw.split('.');
    if (parts.length !== 3) {
      return null;
    }

    // Base64url -> Base64
    let payloadB64 = parts[1].replace(/-/g, '+').replace(/_/g, '/');

    // Pad to multiple of 4
    const pad = payloadB64.length % 4;
    if (pad) {
      payloadB64 += '='.repeat(4 - pad);
    }

    const json = decodeURIComponent(
      atob(payloadB64)
        .split('')
        .map((c) => '%' + ('00' + c.charCodeAt(0).toString(16)).slice(-2))
        .join(''),
    );

    return JSON.parse(json) as JwtPayload;
  } catch {
    return null;
  }
}

/**
 * Check whether a decoded JWT payload is expired.
 */
export function isJwtExpired(payload: JwtPayload): boolean {
  if (!payload.exp) return false;
  // exp is in seconds since epoch
  return payload.exp * 1000 < Date.now();
}

/**
 * Convenience: returns true if the given raw token belongs to an admin user.
 */
export function isAdminToken(token: string | null): boolean {
  if (!token) return false;
  const payload = decodeJwt(token);
  if (!payload) return false;
  if (isJwtExpired(payload)) return false;
  return payload.roles?.toUpperCase().includes('ADMIN') ?? false;
}