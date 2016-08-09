package com.github.wangshichun.jedis;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;
import redis.clients.jedis.HostAndPort;

/**
 * Created by wangshichun on 2016/8/9.
 */
@RunWith(MockitoJUnitRunner.class)
public class JedisClusterTest {
    @Spy
    private JedisCluster jedisCluster = new JedisCluster(new HostAndPort("10.255.209.47", 30001));

    @Test
    public void test() {
        jedisCluster.setReadPreference(ReadPreference.MASTER_THEN_ALL_SLAVE);
        testReal();

        jedisCluster.setReadPreference(ReadPreference.MASTER_ONLY);
        testReal();


        jedisCluster.setReadPreference(ReadPreference.MASTER_THEN_ONE_SLAVE);
        testReal();


        jedisCluster.setReadPreference(ReadPreference.ALL_SLAVE_THEN_MASTER);
        testReal();


        jedisCluster.setReadPreference(ReadPreference.ONE_SLAVE_ONLY);
        testReal();


        jedisCluster.setReadPreference(ReadPreference.ONE_SLAVE_THEN_MASTER);
        testReal();
    }

    private void testReal() {
        jedisCluster.set("test", "testValue");
        Assert.assertTrue("testValue".equals(jedisCluster.get("test")));
    }
}
