package com.exjava.dislock;

import redis.clients.jedis.ShardedJedisPool;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
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

    private final String key;
    private final long ttl;
    private final ShardedJedisPool shardedJedisPool;

    public RedisReentrantReadWriteLock(String key, ShardedJedisPool shardedJedisPool) {
        this(key, 0L, shardedJedisPool);
    }

    public RedisReentrantReadWriteLock(String key, long ttl, ShardedJedisPool shardedJedisPool) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key must not be null or empty string");
        }
        if (shardedJedisPool == null) {
            throw new IllegalArgumentException("sharded jedis pool must not be null");
        }
        this.key = key;
        this.ttl = ttl;
        this.shardedJedisPool = shardedJedisPool;
    }

    @Override
    public Lock readLock() {
        return null;
    }

    @Override
    public Lock writeLock() {
        return null;
    }

    private static class ReadLock implements Lock {

        @Override
        public void lock() {

        }

        @Override
        public void lockInterruptibly() throws InterruptedException {

        }

        @Override
        public boolean tryLock() {
            return false;
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            return false;
        }

        @Override
        public void unlock() {

        }

        @Override
        public Condition newCondition() {
            return null;
        }
    }


    private static class WriteLock {

    }

}
