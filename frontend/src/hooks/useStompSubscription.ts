import { useEffect, useRef } from 'react';
import { wsManager } from '@/lib/websocket';

/**
 * Subscribe to a STOMP destination for the lifetime of the component.
 *
 * @param destination STOMP topic (e.g. "/topic/auction/123")
 * @param handler     Callback invoked with the parsed JSON payload.
 *
 * The subscription is automatically cleaned up on unmount or when the
 * destination/handler identity changes.
 */
export function useStompSubscription<T = unknown>(
  destination: string | null,
  handler: (message: T) => void,
): void {
  // Keep the latest handler in a ref so we don't resubscribe on every render.
  const handlerRef = useRef(handler);
  handlerRef.current = handler;

  useEffect(() => {
    if (!destination) return;

    const stableHandler = (msg: unknown) => handlerRef.current(msg as T);
    const unsubscribe = wsManager.subscribe<T>(destination, stableHandler);

    return () => {
      unsubscribe();
    };
  }, [destination]);
}