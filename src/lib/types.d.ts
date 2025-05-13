
// Type definitions for Edge Runtime
declare namespace EdgeRuntime {
  function waitUntil(promise: Promise<any>): void;
}

// Type definitions for Redis
interface RedisLockStatus {
  acquired: boolean;
  reason?: string;
  method?: string;
  lockId?: string;
}

// Type definitions for Idempotency
interface IdempotencyKey {
  operation: string;
  entity: string;
  id: string;
  timestamp?: number;
}

// Type definitions for Transaction
interface TransactionState {
  id: string;
  status: 'pending' | 'committed' | 'rolledback' | 'error';
  steps: Array<{
    name: string;
    status: 'pending' | 'completed' | 'failed';
    timestamp: number;
  }>;
}
