import { List, Avatar, Typography, Empty } from 'antd';
import { UserOutlined } from '@ant-design/icons';
import type { BidHistoryItem } from '@/types';
import dayjs from 'dayjs';

const { Text } = Typography;

interface BidHistoryListProps {
  bids: BidHistoryItem[];
  loading?: boolean;
}

/**
 * Scrollable list of bid history entries.
 * New bids are prepended by the parent (AuctionDetailPage) in real-time.
 */
export function BidHistoryList({ bids, loading }: BidHistoryListProps) {
  if (!loading && bids.length === 0) {
    return <Empty description="No bids yet" image={Empty.PRESENTED_IMAGE_SIMPLE} />;
  }

  return (
    <List
      loading={loading}
      dataSource={bids}
      renderItem={(item, index) => (
        <List.Item>
          <List.Item.Meta
            avatar={
              <Avatar
                icon={<UserOutlined />}
                style={{ backgroundColor: index === 0 ? '#cf1322' : '#1890ff' }}
              />
            }
            title={
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <Text strong>{item.bidderUsername}</Text>
                <Text strong style={{ color: '#cf1322', fontSize: 16 }}>
                  ${item.amount.toFixed(2)}
                </Text>
              </div>
            }
            description={
              <Text type="secondary">
                {dayjs(item.bidTime).format('YYYY-MM-DD HH:mm:ss')}
              </Text>
            }
          />
        </List.Item>
      )}
    />
  );
}

export default BidHistoryList;