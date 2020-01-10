package com.exjava.dislock;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.exceptions.JedisException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 基于Redis的分布式可重入读写锁
 *
 * 原理:
 * 1 : 获取读锁
 * 1.1 : 获取本地读锁
 *
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
        final ThreadLocal<RWEntrance> rwEntranceThreadLocal = new ThreadLocal<RWEntrance>();
        this.readLock = new ReadLock(key, readLockAtomicity, readWriteLock.readLock(), shardedJedisPool, rwEntranceThreadLocal);
        this.writeLock = new WriteLock(key, writeLockAtomicity, readWriteLock.writeLock(), shardedJedisPool, rwEntranceThreadLocal);
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
            this.lockAcquireScript = "if redis.call('EXISTS', KEYS[1]..':WRITE') == 0 then return redis.call('INCR', KEYS[1]..':READ') else return 0 end";
            this.lockReleaseScript = "if redis.call('EXISTS', KEYS[1]..':READ') == 1 and tonumber(redis.call('GET', KEYS[1]..':READ')) > 0 then return tonumber(redis.call('DECR', KEYS[1]..':READ')) else return 0 end";
        }

        @Override
        public boolean tryLock(ShardedJedis jedis, String value) {
            Jedis shard = jedis.getShard(key);
            Long result = (Long) shard.eval(lockAcquireScript, 1, key);
            return result > 0L;
        }

        @Override
        public void disLock(ShardedJedis jedis, String value) {
            Jedis shard = jedis.getShard(key);
            Long result = (Long) shard.eval(lockReleaseScript, 1, key);
            if (result == 0L) {
                shard.publish(key, value);
            }
        }
    }

    protected static class ReadLock extends RedisReentrantLock {
        protected final ThreadLocal<RWEntrance> rwEntranceThreadLocal;

        protected ReadLock(String key, RedisAtomicity<String> atomicity, Lock lock, ShardedJedisPool shardedJedisPool, ThreadLocal<RWEntrance> rwEntranceThreadLocal) {
            super(key, atomicity, lock, shardedJedisPool);
            this.rwEntranceThreadLocal = rwEntranceThreadLocal;
        }

        @Override
        public void lock() throws JedisException {
            super.lock();
        }

        @Override
        public void lockInterruptibly() throws JedisException, InterruptedException {
            super.lockInterruptibly();
        }

        @Override
        public boolean tryLock() throws JedisException {
            return super.tryLock();
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit) throws JedisException, InterruptedException {
            return super.tryLock(time, unit);
        }

        @Override
        public void unlock() throws JedisException {
            super.unlock();
        }
    }

    protected static class WriteLockAtomicity extends RedisReentrantLock.LockAtomicity {
        protected final String lockAcquireScript;
        protected final String lockReleaseScript;

        protected WriteLockAtomicity(String key, long ttl) {
            super(key, ttl);

            StringBuilder script = new StringBuilder();
            script.append(" readers = redis.call('KEYS', KEYS[1]..':READ:*');");
            script.append(" if");
            script.append("     redis.call('EXISTS', KEYS[1]..':WRITE') == 0");
            script.append(" and");
            script.append("     (#readers == 0 or (#readers == 1 and readers[1] == ARGV[1]))");
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
            Jedis shard = jedis.getShard(key);
            Object result = ttl > 0L
                    ? shard.eval(lockAcquireScript, 1, key, value, String.valueOf(ttl))
                    : shard.eval(lockAcquireScript, 1, key, value);
            return "OK".equals(result);
        }

        @Override
        public void disLock(ShardedJedis jedis, String value) {
            Jedis shard = jedis.getShard(key);
            shard.eval(lockReleaseScript, 1, key, value);
            shard.publish(key, value);
        }
    }

    protected static class WriteLock extends RedisReentrantLock {
        protected final ThreadLocal<RWEntrance> rwEntranceThreadLocal;

        protected WriteLock(String key, RedisAtomicity<String> atomicity, Lock lock, ShardedJedisPool shardedJedisPool, ThreadLocal<RWEntrance> rwEntranceThreadLocal) {
            super(key, atomicity, lock, shardedJedisPool);
            this.rwEntranceThreadLocal = rwEntranceThreadLocal;
        }

        @Override
        public void lock() throws JedisException {
            super.lock();
        }

        @Override
        public void lockInterruptibly() throws JedisException, InterruptedException {
            super.lockInterruptibly();
        }

        @Override
        public boolean tryLock() throws JedisException {
            return super.tryLock();
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit) throws JedisException, InterruptedException {
            return super.tryLock(time, unit);
        }

        @Override
        public void unlock() throws JedisException {
            super.unlock();
        }
    }

    protected static class RWEntrance {

    }
}
