package com.exjava.dislock;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.params.SetParams;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 基于Redis的分布式可重入锁
 *
 * @author Payne 646742615@qq.com
 * 2020/1/6 17:26
 */
public class RedisReentrantLock implements Lock {
    protected final String key;
    protected final RedisAtomicity atomicity;
    protected final TokenGenerator generator;
    protected final Lock lock;
    protected final ShardedJedisPool shardedJedisPool;
    protected final ThreadLocal<Entrance> entranceThreadLocal = new ThreadLocal<Entrance>();

    public RedisReentrantLock(String key, ShardedJedisPool shardedJedisPool) {
        this(key, 0L, shardedJedisPool);
    }

    public RedisReentrantLock(String key, long ttl, ShardedJedisPool shardedJedisPool) {
        this(key, new LockAtomicity(key, ttl), new RandomTokenGenerator(), shardedJedisPool);
    }

    public RedisReentrantLock(String key, TokenGenerator generator, ShardedJedisPool shardedJedisPool) {
        this(key, 0L, generator, shardedJedisPool);
    }

    public RedisReentrantLock(String key, long ttl, TokenGenerator generator, ShardedJedisPool shardedJedisPool) {
        this(key, new LockAtomicity(key, ttl), generator, shardedJedisPool);
    }

    protected RedisReentrantLock(String key, RedisAtomicity atomicity, TokenGenerator generator, ShardedJedisPool shardedJedisPool) {
        this(key, atomicity, generator, new ReentrantLock(), shardedJedisPool);
    }

    protected RedisReentrantLock(String key, RedisAtomicity atomicity, TokenGenerator generator, Lock lock, ShardedJedisPool shardedJedisPool) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key must not be null or empty string");
        }
        if (atomicity == null) {
            throw new IllegalArgumentException("atomicity must not be null");
        }
        if (generator == null) {
            throw new IllegalArgumentException("generator must not be null");
        }
        if (lock == null) {
            throw new IllegalArgumentException("lock must not be null");
        }
        if (shardedJedisPool == null) {
            throw new IllegalArgumentException("sharded jedis pool must not be null");
        }
        this.key = key;
        this.atomicity = atomicity;
        this.generator = generator;
        this.lock = lock;
        this.shardedJedisPool = shardedJedisPool;
    }

    @Override
    public void lock() throws JedisException {
        lock.lock();
        try {
            Entrance entrance = entranceThreadLocal.get();
            if (entrance == null) {
                String token = generator.generate(key);
                Handler handler = new LockHandler(key, token, atomicity, shardedJedisPool);
                handler.acquire();
                entrance = new Entrance(handler);
                entranceThreadLocal.set(entrance);
            }
            entrance.increase();
        } catch (RuntimeException e) {
            lock.unlock();
            throw e;
        } catch (Throwable e) {
            lock.unlock();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void lockInterruptibly() throws JedisException, InterruptedException {
        lock.lockInterruptibly();
        try {
            Entrance entrance = entranceThreadLocal.get();
            if (entrance == null) {
                String token = generator.generate(key);
                Handler handler = new LockInterruptiblyHandler(key, token, atomicity, shardedJedisPool);
                handler.acquire();
                entrance = new Entrance(handler);
                entranceThreadLocal.set(entrance);
            }
            entrance.increase();
        } catch (RuntimeException e) {
            lock.unlock();
            throw e;
        } catch (Throwable e) {
            lock.unlock();
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean tryLock() throws JedisException {
        if (!lock.tryLock()) {
            return false;
        }
        try {
            Entrance entrance = entranceThreadLocal.get();
            if (entrance == null) {
                String token = generator.generate(key);
                Handler handler = new TryLockHandler(key, token, atomicity, shardedJedisPool);
                if (!handler.acquire()) {
                    return false;
                }
                entrance = new Entrance(handler);
                entranceThreadLocal.set(entrance);
            }
            entrance.increase();
            return true;
        } catch (RuntimeException e) {
            lock.unlock();
            throw e;
        } catch (Throwable e) {
            lock.unlock();
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws JedisException, InterruptedException {
        if (!lock.tryLock(time, unit)) {
            return false;
        }
        try {
            Entrance entrance = entranceThreadLocal.get();
            if (entrance == null) {
                long timeout = unit.toMillis(time);
                String token = generator.generate(key);
                Handler handler = timeout > 0L ? new TryLockTimeoutiblyHandler(key, token, atomicity, shardedJedisPool, timeout) : new TryLockHandler(key, token, atomicity, shardedJedisPool);
                if (!handler.acquire()) {
                    return false;
                }
                entrance = new Entrance(handler);
                entranceThreadLocal.set(entrance);
            }
            entrance.increase();
            return true;
        } catch (RuntimeException e) {
            lock.unlock();
            throw e;
        } catch (Throwable e) {
            lock.unlock();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void unlock() throws JedisException {
        try {
            Entrance entrance = entranceThreadLocal.get();
            if (entrance == null) {
                throw new IllegalMonitorStateException();
            }
            if (entrance.decrease() == 0L) {
                entranceThreadLocal.remove();
                Handler handler = entrance.handler;
                handler.release();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    protected static class Entrance {
        protected final Handler handler;
        protected final AtomicLong count = new AtomicLong(0);

        protected Entrance(Handler handler) {
            this.handler = handler;
        }

        protected void increase() {
            count.incrementAndGet();
        }

        protected long decrease() {
            return count.decrementAndGet();
        }
    }

    protected static class LockAtomicity implements RedisAtomicity {
        protected final String key;
        protected final long ttl;

        protected LockAtomicity(String key, long ttl) {
            this.key = key;
            this.ttl = ttl;
        }

        @Override
        public boolean lockup(ShardedJedis jedis, String token) {
            SetParams params = ttl > 0 ? SetParams.setParams().nx().px(ttl) : SetParams.setParams().nx();
            Jedis shard = jedis.getShard(key);
            String result = shard.set(key, token, params);
            return "OK".equals(result);
        }

        @Override
        public void unlock(ShardedJedis jedis, String token) {
            String script = "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end";
            Jedis shard = jedis.getShard(key);
            shard.eval(script, 1, key, token);
            shard.publish(key, token);
        }
    }

    protected abstract static class Handler extends JedisPubSub {
        protected final String key;
        protected final String token;
        protected final RedisAtomicity atomicity;
        protected final ShardedJedis reader;
        protected final ShardedJedis writer;

        protected volatile boolean locked;

        protected Handler(String key, String token, RedisAtomicity atomicity, ShardedJedisPool shardedJedisPool) {
            this.key = key;
            this.token = token;
            this.atomicity = atomicity;
            this.reader = shardedJedisPool.getResource();
            this.writer = shardedJedisPool.getResource();
        }

        protected boolean acquire() {
            try {
                reader.getShard(key).subscribe(this, key);
                return locked;
            } finally {
                if (!locked) {
                    this.release();
                }
            }
        }

        protected void release() {
            if (!writer.getShard(key).isConnected()) {
                return;
            }
            try {
                atomicity.unlock(writer, token);
            } finally {
                locked = false;
                reader.close();
                writer.close();
            }
        }

    }

    protected static class LockHandler extends Handler {

        protected LockHandler(String key, String token, RedisAtomicity atomicity, ShardedJedisPool shardedJedisPool) {
            super(key, token, atomicity, shardedJedisPool);
        }

        @Override
        public void onSubscribe(String channel, int subscribedChannels) {
            onMessage(channel, token);
        }

        @Override
        public void onMessage(String channel, String message) {
            boolean ok = atomicity.lockup(writer, token);
            if (ok) {
                locked = true;
                this.unsubscribe(key);
            }
        }
    }

    protected static class LockInterruptiblyHandler extends Handler {

        protected LockInterruptiblyHandler(String key, String token, RedisAtomicity atomicity, ShardedJedisPool shardedJedisPool) {
            super(key, token, atomicity, shardedJedisPool);
        }

        @Override
        public void onSubscribe(String channel, int subscribedChannels) {
            onMessage(channel, token);
        }

        @Override
        public void onMessage(String channel, String message) {
            boolean ok = atomicity.lockup(writer, token);
            if (ok) {
                locked = true;
                this.unsubscribe(key);
            }
        }
    }

    protected static class TryLockHandler extends Handler {

        protected TryLockHandler(String key, String token, RedisAtomicity atomicity, ShardedJedisPool shardedJedisPool) {
            super(key, token, atomicity, shardedJedisPool);
        }

        @Override
        public void onSubscribe(String channel, int subscribedChannels) {
            locked = atomicity.lockup(writer, token);
            this.unsubscribe(key);
        }
    }

    protected static class TryLockTimeoutiblyHandler extends Handler {
        protected final static Timer TIMER = new Timer(true);

        protected final long timeout;
        protected final TimerTask interrupter = new TimerTask() {
            @Override
            public void run() {
                TryLockTimeoutiblyHandler.this.unsubscribe(key);
            }
        };

        protected TryLockTimeoutiblyHandler(String key, String token, RedisAtomicity atomicity, ShardedJedisPool shardedJedisPool, long timeout) {
            super(key, token, atomicity, shardedJedisPool);
            this.timeout = timeout;
        }

        @Override
        public void onSubscribe(String channel, int subscribedChannels) {
            TIMER.schedule(interrupter, timeout);
            onMessage(channel, token);
        }

        @Override
        public void onMessage(String channel, String message) {
            boolean ok = atomicity.lockup(writer, token);
            if (ok) {
                locked = true;
                this.unsubscribe(key);
            }
        }

        @Override
        public void onUnsubscribe(String channel, int subscribedChannels) {
            interrupter.cancel();
        }
    }

}
