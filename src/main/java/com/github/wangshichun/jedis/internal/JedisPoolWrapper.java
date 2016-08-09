package com.github.wangshichun.jedis.internal;

import redis.clients.jedis.JedisPool;

/**
 * Created by wangshichun on 2016/8/8.
 */
public class JedisPoolWrapper {
    private JedisPool jedisPool;
    private String host;
    private int port;
    private String hostAndPort;
    public JedisPoolWrapper(JedisPool jedisPool, String host, int port) {
        this.jedisPool = jedisPool;
        this.host = host;
        this.port = port;
        this.hostAndPort = host + ":" + port;
    }

    public JedisPool getJedisPool() {
        return jedisPool;
    }

    public void setJedisPool(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getHostAndPort() {
        return hostAndPort;
    }
}
