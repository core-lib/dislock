package com.exjava.dislock;

import redis.clients.jedis.ShardedJedisPool;

import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;

/**
 * 基于Redis的同步代码块
 *
 * @author Payne 646742615@qq.com
 * 2020/1/7 16:41
 */
public class Synchronized {
    private final Lock lock;

    private Synchronized(String mutex, ShardedJedisPool shardedJedisPool) {
        this.lock = new ReentrantLock(mutex, shardedJedisPool);
    }

    public static Synchronized of(String mutex, ShardedJedisPool shardedJedisPool) {
        return new Synchronized(mutex, shardedJedisPool);
    }

    public void run(Runnable runnable) {
        try {
            lock.lock();
            runnable.run();
        } finally {
            lock.unlock();
        }
    }

    public <V> V call(Callable<V> callable) throws Exception {
        try {
            lock.lock();
            return callable.call();
        } finally {
            lock.unlock();
        }
    }

}