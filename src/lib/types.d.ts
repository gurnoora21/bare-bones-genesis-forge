
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
