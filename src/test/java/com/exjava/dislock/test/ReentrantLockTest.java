package com.exjava.dislock.test;

import com.exjava.dislock.ReentrantLock;
import org.junit.Test;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedisPool;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 分布式可重入锁测试
 *
 * @author Payne 646742615@qq.com
 * 2020/1/7 15:25
 */
public class ReentrantLockTest {

    @Test
    public void test() throws Exception {
        String key = "lock";
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(3000);
        ShardedJedisPool pool = new ShardedJedisPool(config, Collections.singletonList(new JedisShardInfo("127.0.0.1", 6379)));
        final ReentrantLock lock = new ReentrantLock(key, pool);
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

}
