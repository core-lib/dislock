package com.exjava.dislock;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 基于Redis的分布式CyclicBarrier
 *
 * @author Payne 646742615@qq.com
 * 2020/1/13 17:29
 */
public class RedisCyclicBarrier extends CyclicBarrier {

    public static void main(String[] args) throws BrokenBarrierException, InterruptedException {
        CyclicBarrier barrier = new CyclicBarrier(1);
        try {
            barrier.await(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            e.printStackTrace();
        }
        barrier.await();
        System.out.println(barrier);
    }

    public RedisCyclicBarrier(int parties) {
        this(parties, null);
    }

    public RedisCyclicBarrier(int parties, Runnable barrierAction) {
        super(parties, barrierAction);
    }

    @Override
    public int getParties() {
        return super.getParties();
    }

    @Override
    public int await() throws InterruptedException, BrokenBarrierException {
        return super.await();
    }

    @Override
    public int await(long timeout, TimeUnit unit) throws InterruptedException, BrokenBarrierException, TimeoutException {
        return super.await(timeout, unit);
    }

    @Override
    public boolean isBroken() {
        return super.isBroken();
    }

    @Override
    public void reset() {
        super.reset();
    }

    @Override
    public int getNumberWaiting() {
        return super.getNumberWaiting();
    }
}
