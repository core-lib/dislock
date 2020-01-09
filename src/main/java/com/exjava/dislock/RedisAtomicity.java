package com.exjava.dislock;

import redis.clients.jedis.ShardedJedis;

/**
 * Redis原子性
 *
 * @author Payne 646742615@qq.com
 * 2020/1/9 13:51
 */
public interface RedisAtomicity<V> {

    /**
     * 尝试加锁
     *
     * @param jedis redis 连接
     * @param value 锁值
     * @return 如果加锁成功则返回{@code true} 否则返回{@code false}
     */
    boolean tryLock(ShardedJedis jedis, V value);

    /**
     * 解锁
     *
     * @param jedis redis 连接
     * @param value 锁值
     */
    void disLock(ShardedJedis jedis, V value);

}
