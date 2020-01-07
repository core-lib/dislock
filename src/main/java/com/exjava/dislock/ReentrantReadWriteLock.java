package com.exjava.dislock;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * 基于Redis的分布式可重入读写锁
 *
 * @author Payne 646742615@qq.com
 * 2020/1/7 16:40
 */
public class ReentrantReadWriteLock implements ReadWriteLock {

    @Override
    public Lock readLock() {
        return null;
    }

    @Override
    public Lock writeLock() {
        return null;
    }
}
