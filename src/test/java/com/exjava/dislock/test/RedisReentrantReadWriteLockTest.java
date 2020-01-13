package com.exjava.dislock.test;

import com.exjava.dislock.RedisReentrantReadWriteLock;
import org.junit.Test;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedisPool;

import java.util.Collections;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * Redis可重入读写锁测试
 *
 * @author Payne 646742615@qq.com
 * 2020/1/9 16:11
 */
public class RedisReentrantReadWriteLockTest {

    @Test
    public void test() throws Exception {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(3000);
        final ShardedJedisPool pool = new ShardedJedisPool(config, Collections.singletonList(new JedisShardInfo("127.0.0.1", 6379, 20 * 1000)));

        ReadWriteLock lock = new RedisReentrantReadWriteLock("test", pool);
        lock.writeLock().lock();
        lock.readLock().lock();
        lock.readLock().lock();
        lock.readLock().lock();
        System.out.println("OK");
        lock.readLock().unlock();
        lock.readLock().unlock();
        lock.readLock().unlock();
        lock.writeLock().unlock();
    }

}
