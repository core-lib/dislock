package com.exjava.dislock.test;

import org.junit.Test;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedisPool;

import java.util.Collections;

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


    }

}
