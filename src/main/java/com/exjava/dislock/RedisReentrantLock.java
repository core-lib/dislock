package com.exjava.dislock;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.ShardedJedisPool;

import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * 基于Redis的分布式可重入锁
 *
 * @author Payne 646742615@qq.com
 * 2020/1/6 17:26
 */
public class RedisReentrantLock implements Lock {
    private final String key;
    private final ShardedJedisPool shardedJedisPool;
    private final ThreadLocal<Entrance> entranceThreadLocal = new ThreadLocal<Entrance>();

    public RedisReentrantLock(String key, ShardedJedisPool shardedJedisPool) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key must not be null or empty string");
        }
        if (shardedJedisPool == null) {
            throw new IllegalArgumentException("sharded jedis pool must not be null");
        }
        this.key = key;
        this.shardedJedisPool = shardedJedisPool;
    }

    @Override
    public void lock() {
        Entrance entrance = entranceThreadLocal.get();
        if (entrance == null) {
            Handler handler = new LockHandler(key, shardedJedisPool);
            handler.acquire();
            entrance = new Entrance(handler);
            entranceThreadLocal.set(entrance);
        }
        entrance.increase();
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }
        Entrance entrance = entranceThreadLocal.get();
        if (entrance == null) {
            Handler handler = new LockInterruptiblyHandler(key, shardedJedisPool);
            handler.acquire();
            entrance = new Entrance(handler);
            entranceThreadLocal.set(entrance);
        }
        entrance.increase();
    }

    @Override
    public boolean tryLock() {
        Entrance entrance = entranceThreadLocal.get();
        if (entrance == null) {
            Handler handler = new TryLockHandler(key, shardedJedisPool);
            if (!handler.acquire()) {
                return false;
            }
            entrance = new Entrance(handler);
            entranceThreadLocal.set(entrance);
        }
        entrance.increase();
        return true;
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }
        Entrance entrance = entranceThreadLocal.get();
        if (entrance == null) {
            long timeout = unit.toMillis(time);
            Handler handler = timeout > 0L ? new TryLockTimeoutiblyHandler(key, shardedJedisPool, timeout) : new TryLockHandler(key, shardedJedisPool);
            if (!handler.acquire()) {
                return false;
            }
            entrance = new Entrance(handler);
            entranceThreadLocal.set(entrance);
        }
        entrance.increase();
        return true;
    }

    @Override
    public void unlock() {
        Entrance entrance = entranceThreadLocal.get();
        if (entrance == null || !entrance.handler.isLocked()) {
            throw new IllegalMonitorStateException();
        }
        if (entrance.decrease() == 0) {
            Handler handler = entrance.handler;
            handler.release();
            entranceThreadLocal.remove();
        }
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    private static class Entrance {
        final Handler handler;
        final AtomicInteger count = new AtomicInteger(0);

        Entrance(Handler handler) {
            this.handler = handler;
        }

        void increase() {
            count.incrementAndGet();
        }

        int decrease() {
            return count.decrementAndGet();
        }
    }

    private abstract static class Handler extends JedisPubSub {
        final String key;
        final Jedis reader;
        final Jedis writer;

        volatile boolean locked;
        final String value = UUID.randomUUID().toString();

        Handler(String key, ShardedJedisPool shardedJedisPool) {
            this.key = key;
            this.reader = shardedJedisPool.getResource().getShard(key);
            this.writer = shardedJedisPool.getResource().getShard(key);
        }

        boolean acquire() {
            reader.subscribe(this, key);
            return locked;
        }

        void release() {
            try {
                String script = "if redis.call(\"get\",KEYS[1]) == ARGV[1] then return redis.call(\"del\",KEYS[1]) else return 0 end";
                writer.eval(script, 1, key, value);
                writer.publish(key, value);
            } finally {
                locked = false;
                reader.close();
                writer.close();
            }
        }

        boolean isLocked() {
            return locked;
        }
    }

    private static class LockHandler extends Handler {

        LockHandler(String key, ShardedJedisPool shardedJedisPool) {
            super(key, shardedJedisPool);
        }

        @Override
        public void onSubscribe(String channel, int subscribedChannels) {
            onMessage(channel, value);
        }

        @Override
        public void onMessage(String channel, String message) {
            if (writer.setnx(key, value) > 0L) {
                locked = true;
                this.unsubscribe(key);
            }
        }
    }

    private static class LockInterruptiblyHandler extends Handler {

        LockInterruptiblyHandler(String key, ShardedJedisPool shardedJedisPool) {
            super(key, shardedJedisPool);
        }

        @Override
        public void onSubscribe(String channel, int subscribedChannels) {
            onMessage(channel, value);
        }

        @Override
        public void onMessage(String channel, String message) {
            if (writer.setnx(key, value) > 0L) {
                locked = true;
                this.unsubscribe(key);
            }
        }
    }

    private static class TryLockHandler extends Handler {

        TryLockHandler(String key, ShardedJedisPool shardedJedisPool) {
            super(key, shardedJedisPool);
        }

        @Override
        public void onSubscribe(String channel, int subscribedChannels) {
            locked = writer.setnx(key, value) > 0L;
            this.unsubscribe(key);
        }
    }

    private static class TryLockTimeoutiblyHandler extends Handler {
        private final static Timer TIMER = new Timer(true);

        private final long timeout;
        private final TimerTask interrupter = new TimerTask() {
            @Override
            public void run() {
                TryLockTimeoutiblyHandler.this.unsubscribe(key);
            }
        };

        TryLockTimeoutiblyHandler(String key, ShardedJedisPool shardedJedisPool, long timeout) {
            super(key, shardedJedisPool);
            this.timeout = timeout;
        }

        @Override
        public void onSubscribe(String channel, int subscribedChannels) {
            TIMER.schedule(interrupter, timeout);
            onMessage(channel, value);
        }

        @Override
        public void onMessage(String channel, String message) {
            if (writer.setnx(key, value) > 0L) {
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
