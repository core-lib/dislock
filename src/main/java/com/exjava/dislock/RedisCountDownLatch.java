package com.exjava.dislock;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 基于Redis的分布式CountDownLatch
 *
 * @author Payne 646742615@qq.com
 * 2020/1/13 17:27
 */
public class RedisCountDownLatch extends CountDownLatch {

    public RedisCountDownLatch(int count) {
        super(count);
    }

    @Override
    public void await() throws InterruptedException {
        super.await();
    }

    @Override
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        return super.await(timeout, unit);
    }

    @Override
    public void countDown() {
        super.countDown();
    }

    @Override
    public long getCount() {
        return super.getCount();
    }
}
