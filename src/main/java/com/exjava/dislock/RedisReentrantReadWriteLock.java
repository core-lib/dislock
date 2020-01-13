package com.exjava.dislock;

import redis.clients.jedis.Jedis;
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
        this(key, new ReadLockAtomicity(key, ttl), new WriteLockAtomicity(key, ttl), new ThreadTokenGenerator(), new ReentrantReadWriteLock(), shardedJedisPool);
    }

    public RedisReentrantReadWriteLock(String key, TokenGenerator generator, ShardedJedisPool shardedJedisPool) {
        this(key, 0L, generator, shardedJedisPool);
    }

    public RedisReentrantReadWriteLock(String key, long ttl, TokenGenerator generator, ShardedJedisPool shardedJedisPool) {
        this(key, new ReadLockAtomicity(key, ttl), new WriteLockAtomicity(key, ttl), generator, new ReentrantReadWriteLock(), shardedJedisPool);
    }

    protected RedisReentrantReadWriteLock(String key, RedisAtomicity readLockAtomicity, RedisAtomicity writeLockAtomicity, TokenGenerator generator, ShardedJedisPool shardedJedisPool) {
        this(key, readLockAtomicity, writeLockAtomicity, generator, new ReentrantReadWriteLock(), shardedJedisPool);
    }

    protected RedisReentrantReadWriteLock(String key, RedisAtomicity readLockAtomicity, RedisAtomicity writeLockAtomicity, TokenGenerator generator, ReadWriteLock readWriteLock, ShardedJedisPool shardedJedisPool) {
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
        this.readLock = new ReadLock(key, readLockAtomicity, generator, readWriteLock.readLock(), shardedJedisPool);
        this.writeLock = new WriteLock(key, writeLockAtomicity, generator, readWriteLock.writeLock(), shardedJedisPool);
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
        protected final String lockupScript;
        protected final String unlockScript;

        protected ReadLockAtomicity(String key, long ttl) {
            super(key, ttl);

            StringBuilder script = new StringBuilder();
            // 如果
            script.append(" if");
            // 写锁不存在
            script.append("     redis.call('EXISTS', KEYS[1]..':WRITE') == 0");
            // 或者
            script.append(" or");
            // 自身线程获取到写锁
            script.append("     redis.call('GET', KEYS[1]..':WRITE') == ARGV[1]");
            // 那么
            script.append(" then");
            // 创建读锁
            if (ttl > 0L) {
                script.append(" return redis.call('SET', KEYS[1]..':READ:'..ARGV[1], ARGV[1], 'NX', 'PX', ARGV[2])");
            } else {
                script.append(" return redis.call('SET', KEYS[1]..':READ:'..ARGV[1], ARGV[1], 'NX')");
            }
            // 否则
            script.append(" else");
            // 直接返回
            script.append("     return 0");
            // 结束
            script.append(" end");
            this.lockupScript = script.toString();

            script.setLength(0);
            // 如果
            script.append(" if");
            // 自身的读锁存在
            script.append("     redis.call('GET', KEYS[1]..':READ:'..ARGV[1]) == ARGV[1]");
            // 那么
            script.append(" then");
            // 删除自身的读锁
            script.append("     redis.call('DEL', KEYS[1]..':READ:'..ARGV[1])");
            // 结束
            script.append(" end");
            // 返回剩下的读锁持有数
            script.append(" return #redis.call('KEYS', KEYS[1]..':READ:*')");
            this.unlockScript = script.toString();
        }

        @Override
        public boolean lockup(ShardedJedis jedis, String value) {
            Jedis shard = jedis.getShard(key);
            Object result = shard.eval(lockupScript, 1, key, value, String.valueOf(ttl));
            return "OK".equals(result);
        }

        @Override
        public void unlock(ShardedJedis jedis, String value) {
            Jedis shard = jedis.getShard(key);
            Long reads = (Long) shard.eval(unlockScript, 1, key, value, String.valueOf(ttl));
            if (reads == 0L) {
                shard.publish(key, value);
            }
        }
    }

    protected static class ReadLock extends RedisReentrantLock {

        protected ReadLock(String key, RedisAtomicity atomicity, TokenGenerator generator, Lock lock, ShardedJedisPool shardedJedisPool) {
            super(key, atomicity, generator, lock, shardedJedisPool);
        }
    }

    protected static class WriteLockAtomicity extends RedisReentrantLock.LockAtomicity {
        protected final String lockupScript;
        protected final String unlockScript;

        protected WriteLockAtomicity(String key, long ttl) {
            super(key, ttl);

            StringBuilder script = new StringBuilder();
            // 如果
            script.append(" if");
            // 不存在写锁
            script.append("     redis.call('EXISTS', KEYS[1]..':WRITE') == 0");
            // 而且
            script.append(" and");
            // 不存在读锁
            script.append("     #redis.call('KEYS', KEYS[1]..':READ:*') == 0");
            // 那么
            script.append(" then");
            // 创建写锁
            if (ttl > 0L) {
                script.append(" return redis.call('SET', KEYS[1]..':WRITE', ARGV[1], 'NX', 'PX', ARGV[2])");
            } else {
                script.append(" return redis.call('SET', KEYS[1]..':WRITE', ARGV[1], 'NX')");
            }
            // 否则
            script.append(" else");
            // 直接返回
            script.append("     return 0");
            // 结束
            script.append(" end");
            this.lockupScript = script.toString();

            script.setLength(0);
            // 如果
            script.append(" if");
            // 自身的写锁存在
            script.append("     redis.call('GET', KEYS[1]..':WRITE') == ARGV[1]");
            // 那么
            script.append(" then");
            // 删除自身的写锁
            script.append("     return redis.call('DEL', KEYS[1]..':WRITE')");
            // 否则
            script.append(" else");
            // 直接返回
            script.append("     return 0");
            // 结束
            script.append(" end");
            this.unlockScript = script.toString();
        }

        @Override
        public boolean lockup(ShardedJedis jedis, String value) {
            Jedis shard = jedis.getShard(key);
            Object result = shard.eval(lockupScript, 1, key, value, String.valueOf(ttl));
            return "OK".equals(result);
        }

        @Override
        public void unlock(ShardedJedis jedis, String value) {
            Jedis shard = jedis.getShard(key);
            shard.eval(unlockScript, 1, key, value, String.valueOf(ttl));
            shard.publish(key, value);
        }
    }

    protected static class WriteLock extends RedisReentrantLock {

        protected WriteLock(String key, RedisAtomicity atomicity, TokenGenerator generator, Lock lock, ShardedJedisPool shardedJedisPool) {
            super(key, atomicity, generator, lock, shardedJedisPool);
        }
    }

}
