package com.exjava.dislock;

import redis.clients.jedis.*;
import redis.clients.jedis.params.SetParams;

import java.util.Collections;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 基于Redis的分布式CountDownLatch
 *
 * @author Payne 646742615@qq.com
 * 2020/1/13 17:27
 */
public class RedisCountDownLatch extends CountDownLatch {
    protected final String key;
    protected final ShardedJedisPool shardedJedisPool;

    public static void main(String[] args) throws InterruptedException {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(3000);
        final ShardedJedisPool pool = new ShardedJedisPool(config, Collections.singletonList(new JedisShardInfo("127.0.0.1", 6379, 20 * 1000)));
        CountDownLatch latch = new RedisCountDownLatch(1, "CountDownLatch", pool);
        latch.await();
        System.out.println("OK");
    }

    public RedisCountDownLatch(int count, String key, ShardedJedisPool shardedJedisPool) {
        this(count, key, 0L, shardedJedisPool);
    }

    public RedisCountDownLatch(int count, String key, long ttl, ShardedJedisPool shardedJedisPool) {
        super(count);
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key must not be null or empty string");
        }
        if (shardedJedisPool == null) {
            throw new IllegalArgumentException("sharded jedis pool must not be null");
        }
        this.key = key;
        this.shardedJedisPool = shardedJedisPool;

        ShardedJedis jedis = shardedJedisPool.getResource();
        try {
            SetParams params = ttl > 0L ? SetParams.setParams().nx().px(ttl) : SetParams.setParams().nx();
            jedis.set(key, String.valueOf(count), params);
        } finally {
            jedis.close();
        }
    }

    @Override
    public void await() throws InterruptedException {
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }
        ShardedJedis jedis = shardedJedisPool.getResource();
        try {
            Jedis shard = jedis.getShard(key);
            AwaitHandler handler = new AwaitHandler(this);
            shard.subscribe(handler, key);
        } finally {
            jedis.close();
        }
    }

    @Override
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }

        ShardedJedis jedis = shardedJedisPool.getResource();
        try {
            Jedis shard = jedis.getShard(key);
            AwaitTimeoutiblyHandler handler = new AwaitTimeoutiblyHandler(this, unit.toMillis(timeout));
            shard.subscribe(handler, key);
            return handler.zeroed.get();
        } finally {
            jedis.close();
        }
    }

    @Override
    public void countDown() {
        StringBuilder script = new StringBuilder();
        script.append(" if");
        script.append("     redis.call('EXISTS', KEYS[1]) == 1");
        script.append(" and");
        script.append("     tonumber(redis.call('GET', KEYS[1])) > 0");
        script.append(" then");
        script.append("     return redis.call('DECR', KEYS[1])");
        script.append(" else");
        script.append("     return 0");
        script.append(" end");
        ShardedJedis jedis = shardedJedisPool.getResource();
        try {
            Jedis shard = jedis.getShard(key);
            Long result = (Long) shard.eval(script.toString(), 1, key);
            if (result == 0L) {
                shard.publish(key, key);
            }
        } finally {
            jedis.close();
        }
    }

    @Override
    public long getCount() {
        ShardedJedis jedis = shardedJedisPool.getResource();
        try {
            String count = jedis.get(key);
            return Long.parseLong(count);
        } finally {
            jedis.close();
        }
    }

    protected static class Handler extends JedisPubSub {
        protected final CountDownLatch latch;

        Handler(CountDownLatch latch) {
            this.latch = latch;
        }
    }

    protected static class AwaitHandler extends Handler {

        AwaitHandler(CountDownLatch latch) {
            super(latch);
        }

        @Override
        public void onSubscribe(String channel, int subscribedChannels) {
            if (latch.getCount() == 0L) {
                this.unsubscribe();
            }
        }

        @Override
        public void onMessage(String channel, String message) {
            this.unsubscribe();
        }
    }

    protected static class AwaitTimeoutiblyHandler extends Handler {
        protected final static Timer TIMER = new Timer(true);
        protected final TimerTask interrupter = new TimerTask() {
            @Override
            public void run() {
                AwaitTimeoutiblyHandler.this.unsubscribe();
            }
        };
        protected final long timeout;
        protected AtomicBoolean zeroed = new AtomicBoolean(false);

        AwaitTimeoutiblyHandler(CountDownLatch latch, long timeout) {
            super(latch);
            this.timeout = timeout;
        }

        @Override
        public void onSubscribe(String channel, int subscribedChannels) {
            if (latch.getCount() == 0L) {
                zeroed.set(true);
                this.unsubscribe();
            } else if (timeout <= 0L) {
                this.unsubscribe();
            } else {
                TIMER.schedule(interrupter, timeout);
            }
        }

        @Override
        public void onMessage(String channel, String message) {
            zeroed.set(true);
            this.unsubscribe();
        }

        @Override
        public void onUnsubscribe(String channel, int subscribedChannels) {
            interrupter.cancel();
        }
    }

}
