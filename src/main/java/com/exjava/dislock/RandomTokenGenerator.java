package com.exjava.dislock;

import java.util.UUID;

/**
 * 随机的令牌生成器
 *
 * @author Payne 646742615@qq.com
 * 2020/1/13 13:54
 */
public class RandomTokenGenerator implements TokenGenerator {

    @Override
    public String generate(String key) {
        return UUID.randomUUID().toString();
    }
}
