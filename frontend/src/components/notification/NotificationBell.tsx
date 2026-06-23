import { useState } from 'react';
import { Badge, Popover, List, Typography, Button, Empty, App } from 'antd';
import { BellOutlined } from '@ant-design/icons';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { listNotifications, markAsRead } from '@/api/notification';
import { useStompSubscription } from '@/hooks/useStompSubscription';
import type { Notification } from '@/types';
import dayjs from 'dayjs';

const { Text } = Typography;

/**
 * Notification bell with unread count badge and dropdown list.
 *
 * Real-time updates are handled globally in App.tsx; this component just
 * refreshes the list when the query cache is invalidated.
 */
export function NotificationBell() {
  const [open, setOpen] = useState(false);
  const queryClient = useQueryClient();
  const { message } = App.useApp();

  const { data: notificationsPage } = useQuery({
    queryKey: ['notifications', 'recent'],
    queryFn: () => listNotifications(0, 10),
    refetchInterval: 30_000,
  });

  const notifications = notificationsPage?.content ?? [];
  const unreadCount = notifications.filter((n) => !n.read).length;

  const markReadMutation = useMutation({
    mutationFn: (id: string) => markAsRead(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['notifications'] });
    },
  });

  // Listen for real-time notifications on a global user channel.
  // The backend pushes to user-specific topics; we listen on a generic channel
  // and invalidate the cache to refresh the bell.
  useStompSubscription('/topic/notifications', () => {
    queryClient.invalidateQueries({ queryKey: ['notifications'] });
    message.info('You have a new notification.');
  });

  const handleMarkAllRead = () => {
    notifications.filter((n) => !n.read).forEach((n) => markReadMutation.mutate(n.id));
  };

  const content = (
    <div style={{ width: 350 }}>
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          padding: '8px 0',
          borderBottom: '1px solid #f0f0f0',
        }}
      >
        <Text strong>Notifications</Text>
        {unreadCount > 0 && (
          <Button type="link" size="small" onClick={handleMarkAllRead}>
            Mark all read
          </Button>
        )}
      </div>

      {notifications.length === 0 ? (
        <Empty
          description="No notifications"
          image={Empty.PRESENTED_IMAGE_SIMPLE}
          style={{ padding: 24 }}
        />
      ) : (
        <List
          dataSource={notifications}
          renderItem={(item: Notification) => (
            <List.Item
              style={{
                background: item.read ? 'transparent' : '#f6ffed',
                padding: '8px 12px',
                cursor: 'pointer',
              }}
              onClick={() => {
                if (!item.read) markReadMutation.mutate(item.id);
              }}
            >
              <List.Item.Meta
                title={
                  <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <Text strong={!item.read}>{item.title}</Text>
                    {!item.read && <Badge status="processing" />}
                  </div>
                }
                description={
                  <>
                    <Text type="secondary" style={{ fontSize: 12 }}>
                      {item.content}
                    </Text>
                    <br />
                    <Text type="secondary" style={{ fontSize: 11 }}>
                      {dayjs(item.createdAt).format('YYYY-MM-DD HH:mm')}
                    </Text>
                  </>
                }
              />
            </List.Item>
          )}
        />
      )}
    </div>
  );

  return (
    <Popover
      content={content}
      trigger="click"
      open={open}
      onOpenChange={setOpen}
      placement="bottomRight"
    >
      <Badge count={unreadCount} size="small">
        <Button type="text" icon={<BellOutlined style={{ fontSize: 18 }} />} />
      </Badge>
    </Popover>
  );
}

export default NotificationBell;