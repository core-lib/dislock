package com.exjava.dislock.test;

import com.exjava.dislock.ReentrantLock;
import org.junit.Test;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedisPool;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

/**
 * 分布式可重入锁测试
 *
 * @author Payne 646742615@qq.com
 * 2020/1/7 15:25
 */
public class ReentrantLockTest {

    @Test
    public void testWithoutTTL() throws Exception {
        String key = "lockWithoutTimeout";
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(3000);
        final ShardedJedisPool pool = new ShardedJedisPool(config, Collections.singletonList(new JedisShardInfo("127.0.0.1", 6379, 20 * 1000)));
        final ReentrantLock lock = new ReentrantLock(key, pool);
        final int[] arr = new int[1];
        for (int i = 0; i < 20000; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    lock.lock();
                    try {
                        System.out.println(arr[0] += 1);
                    } finally {
                        lock.unlock();
                    }
                }
            }).start();
        }

        Thread.sleep(30000);
    }

    @Test
    public void testWithTTL() throws Exception {
        String key = "lockWithTimeout";
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(3000);
        ShardedJedisPool pool = new ShardedJedisPool(config, Collections.singletonList(new JedisShardInfo("127.0.0.1", 6379)));
        final ReentrantLock lock = new ReentrantLock(key, 10 * 1000L, pool);
        final AtomicInteger count = new AtomicInteger(0);
        for (int i = 0; i < 100; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    boolean locked = false;
                    try {
                        if (locked = lock.tryLock(5, TimeUnit.SECONDS)) {
                            Thread.sleep(10000);
                            System.out.println("success#" + count.incrementAndGet());
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
                }
            }).start();
        }

        Thread.sleep(20000);
    }

    @Test
    public void test() throws Exception {
        final Lock lock = new java.util.concurrent.locks.ReentrantLock();
        new Thread(new Runnable() {
            @Override
            public void run() {
                lock.lock();
                lock.unlock();
                lock.unlock();
            }
        }).start();

        Thread.sleep(1000);


        Thread.sleep(10 * 1000);
    }

}
