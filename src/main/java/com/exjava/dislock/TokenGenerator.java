package com.exjava.dislock;

/**
 * 令牌生成器
 *
 * @author Payne 646742615@qq.com
 * 2020/1/13 13:46
 */
public interface TokenGenerator {

    /**
     * 生成令牌
     *
     * @param key 键
     * @return 令牌
     */
    String generate(String key);

}
