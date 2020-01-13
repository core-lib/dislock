package com.exjava.dislock;

import java.util.UUID;

/**
 * 线程的令牌生成器
 *
 * @author Payne 646742615@qq.com
 * 2020/1/13 14:26
 */
public class ThreadTokenGenerator implements TokenGenerator {
    private final String seed = UUID.randomUUID().toString();

    @Override
    public String generate(String key) {
        return seed + "-" + Thread.currentThread().getId();
    }
}
