package com.exjava.dislock;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 基于Redis的分布式可重入读写锁
 *
 * @author Payne 646742615@qq.com
 * 2020/1/7 16:40
 */
public class RedisReentrantReadWriteLock implements ReadWriteLock {
    protected final Lock readLock;
    protected final Lock writeLock;

    public RedisReentrantReadWriteLock(String key, ShardedJedisPool shardedJedisPool) {
        this(key, 0L, shardedJedisPool);
    }

    public RedisReentrantReadWriteLock(String key, long ttl, ShardedJedisPool shardedJedisPool) {
        this(key, new ReadLockAtomicity(key, ttl), new WriteLockAtomicity(key, ttl), new ReentrantReadWriteLock(), shardedJedisPool);
    }

    protected RedisReentrantReadWriteLock(String key, RedisAtomicity<String> readLockAtomicity, RedisAtomicity<String> writeLockAtomicity, ShardedJedisPool shardedJedisPool) {
        this(key, readLockAtomicity, writeLockAtomicity, new ReentrantReadWriteLock(), shardedJedisPool);
    }

    protected RedisReentrantReadWriteLock(String key, RedisAtomicity<String> readLockAtomicity, RedisAtomicity<String> writeLockAtomicity, ReadWriteLock readWriteLock, ShardedJedisPool shardedJedisPool) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key must not be null or empty string");
        }
        if (shardedJedisPool == null) {
            throw new IllegalArgumentException("sharded jedis pool must not be null");
        }
        if (readLockAtomicity == null) {
            throw new IllegalArgumentException("read lock atomicity must not be null");
        }
        if (writeLockAtomicity == null) {
            throw new IllegalArgumentException("write lock atomicity must not be null");
        }
        this.readLock = new ReadLock(key, readLockAtomicity, readWriteLock.readLock(), shardedJedisPool);
        this.writeLock = new WriteLock(key, writeLockAtomicity, readWriteLock.writeLock(), shardedJedisPool);
    }

    @Override
    public Lock readLock() {
        return readLock;
    }

    @Override
    public Lock writeLock() {
        return writeLock;
    }

    protected static class ReadLockAtomicity extends RedisReentrantLock.LockAtomicity {
        protected final String lockAcquireScript;
        protected final String lockReleaseScript;

        protected ReadLockAtomicity(String key, long ttl) {
            super(key, ttl);

            StringBuilder script = new StringBuilder();
            script.append(" if");
            script.append("     redis.call('EXISTS', KEYS[1]..':WRITE') == 0");
            script.append(" then");
            if (ttl > 0L) {
                script.append(" return redis.call('INCR', KEYS[1]..':READ', ARGV[1], 'NX', 'PX', ARGV[2])");
            } else {
                script.append(" return redis.call('SET', KEYS[1]..':WRITE', ARGV[1], 'NX')");
            }
            script.append(" else");
            script.append("     return 0");
            script.append(" end");
            this.lockAcquireScript = script.toString();

            this.lockReleaseScript = "if redis.call('EXISTS', KEYS[1]) == 1 and redis.call('GET', KEYS[1]..':READ') > 0 then return redis.call('DEL', KEYS[1]..':WRITE') else return 0 end";
        }

        @Override
        public boolean tryLock(ShardedJedis jedis, String value) {
            return false;
        }

        @Override
        public void disLock(ShardedJedis jedis, String value) {

        }
    }

    protected static class ReadLock extends RedisReentrantLock {

        public ReadLock(String key, RedisAtomicity<String> atomicity, Lock lock, ShardedJedisPool shardedJedisPool) {
            super(key, atomicity, lock, shardedJedisPool);
        }
    }

    protected static class WriteLockAtomicity extends RedisReentrantLock.LockAtomicity {
        protected final String lockAcquireScript;
        protected final String lockReleaseScript;

        protected WriteLockAtomicity(String key, long ttl) {
            super(key, ttl);

            StringBuilder script = new StringBuilder();
            script.append(" if");
            script.append("     redis.call('EXISTS', KEYS[1]..':WRITE') == 0");
            script.append(" and");
            script.append("     (redis.call('EXISTS', KEYS[1]..':READ') == 0 or redis.call('GET', KEYS[1]..':READ') == 0)");
            script.append(" then");
            if (ttl > 0L) {
                script.append(" return redis.call('SET', KEYS[1]..':WRITE', ARGV[1], 'NX', 'PX', ARGV[2])");
            } else {
                script.append(" return redis.call('SET', KEYS[1]..':WRITE', ARGV[1], 'NX')");
            }
            script.append(" else");
            script.append("     return 0");
            script.append(" end");
            this.lockAcquireScript = script.toString();

            this.lockReleaseScript = "if redis.call('GET', KEYS[1]..':WRITE') == ARGV[1] then return redis.call('DEL', KEYS[1]..':WRITE') else return 0 end";
        }

        @Override
        public boolean tryLock(ShardedJedis jedis, String value) {
            Object result = ttl > 0L
                    ? jedis.getShard(key).eval(lockAcquireScript, 1, key, value, String.valueOf(ttl))
                    : jedis.getShard(key).eval(lockAcquireScript, 1, key, value);
            return "OK".equals(result);
        }

        @Override
        public void disLock(ShardedJedis jedis, String value) {
            jedis.getShard(key).eval(lockReleaseScript, 1, key, value);
        }
    }

    protected static class WriteLock extends RedisReentrantLock {

        public WriteLock(String key, RedisAtomicity<String> atomicity, Lock lock, ShardedJedisPool shardedJedisPool) {
            super(key, atomicity, lock, shardedJedisPool);
        }
    }
}
