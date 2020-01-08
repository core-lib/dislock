package com.exjava.dislock;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.params.SetParams;

import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * 基于Redis的分布式可重入锁
 *
 * @author Payne 646742615@qq.com
 * 2020/1/6 17:26
 */
public class ReentrantLock implements Lock {
    private final String key;
    private final long ttl;
    private final ShardedJedisPool shardedJedisPool;
    private final ThreadLocal<Entrance> entranceThreadLocal = new ThreadLocal<Entrance>();

    public ReentrantLock(String key, ShardedJedisPool shardedJedisPool) {
        this(key, 0, shardedJedisPool);
    }

    public ReentrantLock(String key, long ttl, ShardedJedisPool shardedJedisPool) {
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
    public void lock() throws JedisException {
        Entrance entrance = entranceThreadLocal.get();
        if (entrance == null) {
            Handler handler = new LockHandler(key, ttl, shardedJedisPool);
            handler.acquire();
            entrance = new Entrance(handler);
            entranceThreadLocal.set(entrance);
        }
        entrance.increase();
    }

    @Override
    public void lockInterruptibly() throws JedisException, InterruptedException {
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }
        Entrance entrance = entranceThreadLocal.get();
        if (entrance == null) {
            Handler handler = new LockInterruptiblyHandler(key, ttl, shardedJedisPool);
            handler.acquire();
            entrance = new Entrance(handler);
            entranceThreadLocal.set(entrance);
        }
        entrance.increase();
    }

    @Override
    public boolean tryLock() throws JedisException {
        Entrance entrance = entranceThreadLocal.get();
        if (entrance == null) {
            Handler handler = new TryLockHandler(key, ttl, shardedJedisPool);
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
    public boolean tryLock(long time, TimeUnit unit) throws JedisException, InterruptedException {
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }
        Entrance entrance = entranceThreadLocal.get();
        if (entrance == null) {
            long timeout = unit.toMillis(time);
            Handler handler = timeout > 0L ? new TryLockTimeoutiblyHandler(key, ttl, shardedJedisPool, timeout) : new TryLockHandler(key, ttl, shardedJedisPool);
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
    public void unlock() throws JedisException {
        Entrance entrance = entranceThreadLocal.get();
        if (entrance == null) {
            throw new IllegalMonitorStateException();
        }
        if (entrance.decrease() == 0L) {
            entranceThreadLocal.remove();
            Handler handler = entrance.handler;
            handler.release();
        }
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    private static class Entrance {
        final Handler handler;
        final AtomicLong count = new AtomicLong(0);

        Entrance(Handler handler) {
            this.handler = handler;
        }

        void increase() {
            count.incrementAndGet();
        }

        long decrease() {
            return count.decrementAndGet();
        }
    }

    private abstract static class Handler extends JedisPubSub {
        final String key;
        final SetParams params;
        final Jedis reader;
        final Jedis writer;

        volatile boolean locked;
        final String value = UUID.randomUUID().toString();

        Handler(String key, long ttl, ShardedJedisPool shardedJedisPool) {
            this.key = key;
            this.params = ttl > 0 ? SetParams.setParams().nx().px(ttl) : SetParams.setParams().nx();
            this.reader = shardedJedisPool.getResource().getShard(key);
            this.writer = shardedJedisPool.getResource().getShard(key);
        }

        boolean acquire() {
            try {
                reader.subscribe(this, key);
                return locked;
            } finally {
                if (!locked) {
                    this.release();
                }
            }
        }

        void release() {
            if (!writer.isConnected()) {
                return;
            }
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

    }

    private static class LockHandler extends Handler {

        LockHandler(String key, long ttl, ShardedJedisPool shardedJedisPool) {
            super(key, ttl, shardedJedisPool);
        }

        @Override
        public void onSubscribe(String channel, int subscribedChannels) {
            onMessage(channel, value);
        }

        @Override
        public void onMessage(String channel, String message) {
            String result = writer.set(key, value, params);
            if ("OK".equals(result)) {
                locked = true;
                this.unsubscribe(key);
            }
        }
    }

    private static class LockInterruptiblyHandler extends Handler {

        LockInterruptiblyHandler(String key, long ttl, ShardedJedisPool shardedJedisPool) {
            super(key, ttl, shardedJedisPool);
        }

        @Override
        public void onSubscribe(String channel, int subscribedChannels) {
            onMessage(channel, value);
        }

        @Override
        public void onMessage(String channel, String message) {
            String result = writer.set(key, value, params);
            if ("OK".equals(result)) {
                locked = true;
                this.unsubscribe(key);
            }
        }
    }

    private static class TryLockHandler extends Handler {

        TryLockHandler(String key, long ttl, ShardedJedisPool shardedJedisPool) {
            super(key, ttl, shardedJedisPool);
        }

        @Override
        public void onSubscribe(String channel, int subscribedChannels) {
            String result = writer.set(key, value, params);
            locked = "OK".equals(result);
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

        TryLockTimeoutiblyHandler(String key, long ttl, ShardedJedisPool shardedJedisPool, long timeout) {
            super(key, ttl, shardedJedisPool);
            this.timeout = timeout;
        }

        @Override
        public void onSubscribe(String channel, int subscribedChannels) {
            TIMER.schedule(interrupter, timeout);
            onMessage(channel, value);
        }

        @Override
        public void onMessage(String channel, String message) {
            String result = writer.set(key, value, params);
            if ("OK".equals(result)) {
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
