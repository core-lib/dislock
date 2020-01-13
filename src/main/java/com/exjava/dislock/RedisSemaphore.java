package com.exjava.dislock;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * 基于Redis的分布式Semaphore
 *
 * @author Payne 646742615@qq.com
 * 2020/1/13 16:58
 */
public class RedisSemaphore extends Semaphore {

    public RedisSemaphore(int permits) {
        super(permits);
    }

    @Override
    public void acquire() throws InterruptedException {
        super.acquire();
    }

    @Override
    public void acquireUninterruptibly() {
        super.acquireUninterruptibly();
    }

    @Override
    public boolean tryAcquire() {
        return super.tryAcquire();
    }

    @Override
    public boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
        return super.tryAcquire(timeout, unit);
    }

    @Override
    public void release() {
        super.release();
    }

    @Override
    public void acquire(int permits) throws InterruptedException {
        super.acquire(permits);
    }

    @Override
    public void acquireUninterruptibly(int permits) {
        super.acquireUninterruptibly(permits);
    }

    @Override
    public boolean tryAcquire(int permits) {
        return super.tryAcquire(permits);
    }

    @Override
    public boolean tryAcquire(int permits, long timeout, TimeUnit unit) throws InterruptedException {
        return super.tryAcquire(permits, timeout, unit);
    }

    @Override
    public void release(int permits) {
        super.release(permits);
    }

    @Override
    public int availablePermits() {
        return super.availablePermits();
    }

    @Override
    public int drainPermits() {
        return super.drainPermits();
    }

    @Override
    public boolean isFair() {
        return super.isFair();
    }
}
