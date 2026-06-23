import { get, put } from './client';
import type { Notification, SpringPage } from '@/types';

/**
 * GET /api/notifications?page=&size=
 */
export function listNotifications(page = 0, size = 20): Promise<SpringPage<Notification>> {
  return get<SpringPage<Notification>>('/api/notifications', { page, size });
}

/**
 * PUT /api/notifications/{id}/read
 */
export function markAsRead(id: string): Promise<Notification> {
  return put<Notification>(`/api/notifications/${id}/read`);
}