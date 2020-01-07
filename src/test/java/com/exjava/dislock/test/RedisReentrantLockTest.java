package com.exjava.dislock.test;

import com.exjava.dislock.RedisReentrantLock;
import org.junit.Test;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedisPool;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Payne 646742615@qq.com
 * 2020/1/7 15:25
 */
public class RedisReentrantLockTest {

    @Test
    public void test() throws Exception {
        String key = "lock";
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(3000);
        ShardedJedisPool pool = new ShardedJedisPool(config, Collections.singletonList(new JedisShardInfo("127.0.0.1", 6379)));
        RedisReentrantLock lock = new RedisReentrantLock(key, pool);
        AtomicInteger count = new AtomicInteger(0);
        for (int i = 0; i < 100; i++) {
            new Thread(() -> {
                boolean locked = false;
                try {
                    if (locked = lock.tryLock(-2, TimeUnit.MILLISECONDS)) {
                        Thread.sleep(1000);
                    } else {
                        System.out.println("fail#" + count.incrementAndGet());
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } finally {
                    if (locked) {
                        lock.unlock();
                    }
                }
            }).start();
        }

        Thread.sleep(20000);
    }

}