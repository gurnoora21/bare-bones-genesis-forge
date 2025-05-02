
/**
 * Simple LRU cache implementation to reduce Redis hits
 */

interface CacheEntry<T> {
  value: T;
  expiry: number | null;
  lastAccessed: number;
}

export class MemoryCache<T> {
  private cache: Map<string, CacheEntry<T>>;
  private maxSize: number;
  private syncInterval: number;
  private lastSyncTime: number;
  
  constructor(maxSize = 1000, syncIntervalMs = 60000) {
    this.cache = new Map();
    this.maxSize = maxSize;
    this.syncInterval = syncIntervalMs;
    this.lastSyncTime = Date.now();
    
    // Cleanup expired items periodically
    if (typeof setInterval !== 'undefined') {
      setInterval(() => this.cleanup(), 30000);
    }
  }
  
  /**
   * Get a value from cache
   * Returns null/undefined if not found or expired
   */
  get(key: string): T | null | undefined {
    const entry = this.cache.get(key);
    const now = Date.now();
    
    if (!entry) {
      return null;
    }
    
    // Check if expired
    if (entry.expiry && entry.expiry < now) {
      this.cache.delete(key);
      return null;
    }
    
    // Update last accessed time
    entry.lastAccessed = now;
    return entry.value;
  }
  
  /**
   * Store a value in cache
   */
  set(key: string, value: T, ttlSeconds?: number): void {
    // Ensure we don't exceed max size
    if (this.cache.size >= this.maxSize) {
      this.evictOldest();
    }
    
    const now = Date.now();
    const expiry = ttlSeconds ? now + (ttlSeconds * 1000) : null;
    
    this.cache.set(key, {
      value,
      expiry,
      lastAccessed: now
    });
  }
  
  /**
   * Remove a key from cache
   */
  delete(key: string): boolean {
    return this.cache.delete(key);
  }
  
  /**
   * Check if it's time to sync with Redis
   */
  shouldSync(): boolean {
    const now = Date.now();
    if (now - this.lastSyncTime >= this.syncInterval) {
      this.lastSyncTime = now;
      return true;
    }
    return false;
  }
  
  /**
   * Get cache size
   */
  size(): number {
    return this.cache.size;
  }
  
  /**
   * Reset the sync timer
   */
  resetSyncTimer(): void {
    this.lastSyncTime = Date.now();
  }
  
  /**
   * Clear all entries
   */
  clear(): void {
    this.cache.clear();
  }
  
  /**
   * Get all keys in the cache
   */
  keys(): string[] {
    return Array.from(this.cache.keys());
  }
  
  /**
   * Remove oldest entries based on last access time
   */
  private evictOldest(): void {
    if (this.cache.size === 0) return;
    
    let oldestKey: string | null = null;
    let oldestTime = Infinity;
    
    for (const [key, entry] of this.cache.entries()) {
      if (entry.lastAccessed < oldestTime) {
        oldestTime = entry.lastAccessed;
        oldestKey = key;
      }
    }
    
    if (oldestKey) {
      this.cache.delete(oldestKey);
    }
  }
  
  /**
   * Remove expired entries
   */
  private cleanup(): void {
    const now = Date.now();
    for (const [key, entry] of this.cache.entries()) {
      if (entry.expiry && entry.expiry < now) {
        this.cache.delete(key);
      }
    }
  }
}
