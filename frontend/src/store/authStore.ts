import { create } from 'zustand';
import { TOKEN_STORAGE_KEY } from '@/api/client';
import { decodeJwt, isJwtExpired } from '@/lib/jwt';
import type { User } from '@/types';

interface AuthState {
  token: string | null;
  user: User | null;
  isAdmin: boolean;
  isAuthenticated: boolean;

  /** Persist token to localStorage + derive flags. */
  setToken: (token: string) => void;

  /** Set the user profile (fetched from /api/users/me). */
  setUser: (user: User) => void;

  /** Clear all auth state (logout). */
  clear: () => void;

  /** Initialize from localStorage on app startup. */
  hydrate: () => void;
}

export const useAuthStore = create<AuthState>((set) => ({
  token: null,
  user: null,
  isAdmin: false,
  isAuthenticated: false,

  setToken: (token: string) => {
    localStorage.setItem(TOKEN_STORAGE_KEY, token);
    const payload = decodeJwt(token);
    const expired = payload ? isJwtExpired(payload) : true;

    if (expired || !payload) {
      set({ token: null, user: null, isAdmin: false, isAuthenticated: false });
      return;
    }

    set({
      token,
      isAuthenticated: true,
      isAdmin: (payload.roles || '').toUpperCase().includes('ADMIN'),
      // Lightweight user derived from JWT; will be enriched by /api/users/me.
      user: {
        id: Number(payload.sub),
        username: payload.username,
        email: '',
        role: payload.roles,
        createdAt: '',
      },
    });
  },

  setUser: (user: User) => {
    set((state) => ({
      user,
      isAdmin: (user.role || '').toUpperCase().includes('ADMIN') || state.isAdmin,
    }));
  },

  clear: () => {
    localStorage.removeItem(TOKEN_STORAGE_KEY);
    set({ token: null, user: null, isAdmin: false, isAuthenticated: false });
  },

  hydrate: () => {
    const token = localStorage.getItem(TOKEN_STORAGE_KEY);
    if (!token) {
      set({ token: null, user: null, isAdmin: false, isAuthenticated: false });
      return;
    }
    const payload = decodeJwt(token);
    const expired = payload ? isJwtExpired(payload) : true;
    if (expired || !payload) {
      localStorage.removeItem(TOKEN_STORAGE_KEY);
      set({ token: null, user: null, isAdmin: false, isAuthenticated: false });
      return;
    }
    set({
      token,
      isAuthenticated: true,
      isAdmin: (payload.roles || '').toUpperCase().includes('ADMIN'),
      user: {
        id: Number(payload.sub),
        username: payload.username,
        email: '',
        role: payload.roles,
        createdAt: '',
      },
    });
  },
}));