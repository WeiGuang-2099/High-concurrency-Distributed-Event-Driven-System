import { useEffect, useState } from 'react';
import { Statistic } from 'antd';
import type { CountdownProps } from 'antd';

interface CountdownTimerProps {
  /** Target end time as remaining seconds from server or ISO string. */
  remainingSeconds?: number;
  endTime?: string;
  /** Callback when the countdown reaches zero. */
  onFinish?: () => void;
}

const { Countdown } = Statistic;

/**
 * Client-side countdown timer.
 *
 * The backend provides `remainingSeconds` (seconds until endTime).
 * This component ticks every second and renders an Ant Design Countdown.
 */
export function CountdownTimer({ remainingSeconds, endTime, onFinish }: CountdownTimerProps) {
  const [deadline, setDeadline] = useState<number>(0);

  useEffect(() => {
    if (endTime) {
      setDeadline(new Date(endTime).getTime());
    } else if (remainingSeconds !== undefined) {
      setDeadline(Date.now() + remainingSeconds * 1000);
    }
  }, [remainingSeconds, endTime]);

  const handleFinish: CountdownProps['onFinish'] = () => {
    onFinish?.();
  };

  return <Countdown value={deadline} onFinish={handleFinish} />;
}

export default CountdownTimer;