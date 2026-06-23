import { Client, type StompSubscription } from '@stomp/stompjs';
import SockJS from 'sockjs-client';
import { TOKEN_STORAGE_KEY } from '@/api/client';

/**
 * STOMP over SockJS client singleton.
 *
 * The backend notification-service exposes:
 *   Endpoint: /ws/notifications (with SockJS)
 *   Auth: STOMP CONNECT frame must carry "Authorization: Bearer <token>"
 *
 * The Vite dev server proxies /ws/** to the gateway on port 8080.
 */

type MessageHandler<T = unknown> = (message: T) => void;

class WebSocketManager {
  private client: Client | null = null;
  private subscriptions = new Map<string, StompSubscription>();
  private handlers = new Map<string, Set<MessageHandler>>();
  private reconnectAttempts = 0;
  private maxReconnectAttempts = 10;
  private reconnectInterval = 5000;

  /**
   * Establish a STOMP connection using the token from localStorage.
   */
  connect(token: string): Promise<void> {
    return new Promise((resolve, reject) => {
      // If already connected, resolve immediately.
      if (this.client?.active) {
        resolve();
        return;
      }

      const wsBaseUrl = import.meta.env.VITE_WS_BASE_URL || '';
      const wsUrl = `${wsBaseUrl}/ws/notifications`;

      this.client = new Client({
        webSocketFactory: () => new SockJS(wsUrl),
        connectHeaders: {
          Authorization: `Bearer ${token}`,
        },
        reconnectDelay: this.reconnectInterval,
        heartbeatIncoming: 10000,
        heartbeatOutgoing: 10000,

        onConnect: () => {
          this.reconnectAttempts = 0;
          // Re-subscribe to all active topics.
          this.handlers.forEach((handlerSet, destination) => {
            this.subscribeInternal(destination, handlerSet);
          });
          resolve();
        },

        onStompError: (frame) => {
          console.error('[WS] STOMP error:', frame.headers['message'], frame.body);
          reject(new Error(frame.headers['message'] || 'STOMP error'));
        },

        onWebSocketClose: () => {
          this.reconnectAttempts++;
          if (this.reconnectAttempts > this.maxReconnectAttempts) {
            console.warn(
              `[WS] Max reconnect attempts (${this.maxReconnectAttempts}) reached, giving up.`,
            );
            this.deactivate();
          }
        },

        onWebSocketError: (event) => {
          console.error('[WS] WebSocket error:', event);
        },
      });

      this.client.activate();
    });
  }

  /**
   * Subscribe to a STOMP destination with a typed handler.
   * Returns an unsubscribe function.
   */
  subscribe<T = unknown>(destination: string, handler: MessageHandler<T>): () => void {
    // Track handler.
    let handlerSet = this.handlers.get(destination);
    if (!handlerSet) {
      handlerSet = new Set<MessageHandler>();
      this.handlers.set(destination, handlerSet);
    }
    handlerSet.add(handler as MessageHandler);

    // If client is connected, subscribe immediately.
    if (this.client?.active && !this.subscriptions.has(destination)) {
      this.subscribeInternal(destination, handlerSet);
    }

    return () => this.unsubscribe(destination, handler as MessageHandler);
  }

  /**
   * Remove a handler from a destination. If no handlers remain, unsubscribe.
   */
  unsubscribe(destination: string, handler: MessageHandler): void {
    const handlerSet = this.handlers.get(destination);
    if (handlerSet) {
      handlerSet.delete(handler);
      if (handlerSet.size === 0) {
        this.handlers.delete(destination);
        const sub = this.subscriptions.get(destination);
        if (sub) {
          sub.unsubscribe();
          this.subscriptions.delete(destination);
        }
      }
    }
  }

  /**
   * Disconnect and clean up.
   */
  deactivate(): void {
    this.subscriptions.forEach((sub) => sub.unsubscribe());
    this.subscriptions.clear();
    this.handlers.clear();
    if (this.client) {
      this.client.deactivate();
      this.client = null;
    }
  }

  /**
   * Check if the client is currently connected.
   */
  get isConnected(): boolean {
    return this.client?.active ?? false;
  }

  /**
   * Internal: create a STOMP subscription for a destination.
   */
  private subscribeInternal(destination: string, handlerSet: Set<MessageHandler>): void {
    if (!this.client?.active) return;
    if (this.subscriptions.has(destination)) return;

    const sub = this.client.subscribe(destination, (message) => {
      try {
        const payload = JSON.parse(message.body);
        handlerSet.forEach((h) => h(payload));
      } catch (e) {
        console.error(`[WS] Failed to parse message on ${destination}:`, e);
      }
    });
    this.subscriptions.set(destination, sub);
  }
}

export const wsManager = new WebSocketManager();

/**
 * Convenience: read token from localStorage and connect.
 */
export function connectWebSocket(): Promise<void> {
  const token = localStorage.getItem(TOKEN_STORAGE_KEY);
  if (!token) {
    return Promise.reject(new Error('No token available for WebSocket auth'));
  }
  return wsManager.connect(token);
}