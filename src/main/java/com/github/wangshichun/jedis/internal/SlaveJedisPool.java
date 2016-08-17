package com.github.wangshichun.jedis.internal;


import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.JedisPool;

/**
 * Created by wangshichun on 2016/8/17.
 */
class SlaveJedisPool extends JedisPool {
    public SlaveJedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port,
                          final int connectionTimeout, final int soTimeout, final String password, final int database,
                          final String clientName) {
        super(poolConfig, host, port, connectionTimeout, soTimeout, password, database, clientName);
        initPool(poolConfig, new SlaveJedisFactory(host, port, connectionTimeout, soTimeout, password,
                database, clientName));
    }
}
