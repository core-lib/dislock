package com.exjava.dislock.test;

import com.exjava.dislock.Synchronized;
import org.junit.Test;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedisPool;

import java.util.Collections;

/**
 * 分布式同步代码块测试
 *
 * @author Payne 646742615@qq.com
 * 2020/1/7 16:53
 */
public class SynchronizedTest {

    @Test
    public void test() throws Exception {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(3000);
        ShardedJedisPool pool = new ShardedJedisPool(config, Collections.singletonList(new JedisShardInfo("127.0.0.1", 6379)));
        final Synchronized sync = Synchronized.of("mutex", pool);
        final int[] arr = new int[1];
        for (int i = 0; i < 1000; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    sync.run(new Runnable() {
                        @Override
                        public void run() {
                            arr[0] += 1;
                        }
                    });
                }
            }).start();
        }
        Thread.sleep(10000);
        System.out.println(arr[0]);
    }

}
