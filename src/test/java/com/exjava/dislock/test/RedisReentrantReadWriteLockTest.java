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
        long ttl = -10 * 1000L;
        StringBuilder script = new StringBuilder();
        script.append(" if");
        script.append("     redis.call('EXISTS', KEYS[1]..':WRITE') == 0");
        script.append(" and");
        script.append("     (redis.call('EXISTS', KEYS[1]..':READ') == 0 or redis.call('GET', KEYS[1]..':READ') == 0)");
        script.append(" then");
        if (ttl > 0L) {
            script.append(" return redis.call('SET', KEYS[1]..':WRITE', ARGV[1], 'NX', 'PX', ARGV[2])");
        } else {
            script.append(" return redis.call('SET', KEYS[1]..':WRITE', ARGV[1], 'NX')");
        }
        script.append(" else");
        script.append("     return 0");
        script.append(" end");

        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(3000);
        final ShardedJedisPool pool = new ShardedJedisPool(config, Collections.singletonList(new JedisShardInfo("127.0.0.1", 6379, 20 * 1000)));
        String key = "ReadWriteLock";
        String value = "this";
        Object result = pool.getResource().getShard(key).eval(script.toString(), 1, key, value, String.valueOf(ttl));
        System.out.println(result);
    }

}
