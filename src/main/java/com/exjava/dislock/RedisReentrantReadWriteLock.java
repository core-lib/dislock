package com.exjava.dislock;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * 基于Redis的分布式可重入读写锁
 *
 * @author Payne 646742615@qq.com
 * 2020/1/7 16:40
 */
public class RedisReentrantReadWriteLock implements ReadWriteLock {
    private final static String SCRIPT_WRITE_LOCK_ACQUIRE = "if (redis.call('EXISTS', KEYS[1]) == 0 or redis.call('GET', KEYS[1]) == 0) and (redis.call('EXISTS', KEYS[1]) == 0 or redis.call('GET', KEYS[1]) == 0) then return redis.call('INCR', KEYS[1]) else return 0 end";
    private final static String SCRIPT_WRITE_LOCK_RELEASE = "if (redis.call('EXISTS', KEYS[1]) == 0 or redis.call('GET', KEYS[1]) == 0) and (redis.call('EXISTS', KEYS[1]) == 0 or redis.call('GET', KEYS[1]) == 0) then return redis.call('INCR', KEYS[1]) else return 0 end";

    private final static String SCRIPT_READ_LOCK_ACQUIRE = "if (redis.call('EXISTS', KEYS[1]) == 0 or redis.call('GET', KEYS[1]) == 0) and (redis.call('EXISTS', KEYS[1]) == 0 or redis.call('GET', KEYS[1]) == 0) then return redis.call('INCR', KEYS[1]) else return 0 end";
    private final static String SCRIPT_READ_LOCK_RELEASE = "if (redis.call('EXISTS', KEYS[1]) == 0 or redis.call('GET', KEYS[1]) == 0) and (redis.call('EXISTS', KEYS[1]) == 0 or redis.call('GET', KEYS[1]) == 0) then return redis.call('INCR', KEYS[1]) else return 0 end";

    protected final Lock readLock;
    protected final Lock writeLock;

    public RedisReentrantReadWriteLock(String key, ShardedJedisPool shardedJedisPool) {
        this(key, 0L, shardedJedisPool);
    }

    public RedisReentrantReadWriteLock(String key, long ttl, ShardedJedisPool shardedJedisPool) {
        this(key, new ReadLockAtomicity(key, ttl), new WriteLockAtomicity(key, ttl), shardedJedisPool);
    }

    protected RedisReentrantReadWriteLock(String key, RedisAtomicity<String> readLockAtomicity, RedisAtomicity<String> writeLockAtomicity, ShardedJedisPool shardedJedisPool) {
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
        this.readLock = new ReadLock(key, readLockAtomicity, shardedJedisPool);
        this.writeLock = new WriteLock(key, writeLockAtomicity, shardedJedisPool);
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

        protected ReadLockAtomicity(String key, long ttl) {
            super(key, ttl);
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

        protected ReadLock(String key, RedisAtomicity<String> atomicity, ShardedJedisPool shardedJedisPool) {
            super(key, atomicity, shardedJedisPool);
        }

    }

    protected static class WriteLockAtomicity extends RedisReentrantLock.LockAtomicity {

        protected WriteLockAtomicity(String key, long ttl) {
            super(key, ttl);
        }

        @Override
        public boolean tryLock(ShardedJedis jedis, String value) {
            return false;
        }

        @Override
        public void disLock(ShardedJedis jedis, String value) {

        }
    }

    protected static class WriteLock extends RedisReentrantLock {

        protected WriteLock(String key, RedisAtomicity<String> atomicity, ShardedJedisPool shardedJedisPool) {
            super(key, atomicity, shardedJedisPool);
        }

    }
}
