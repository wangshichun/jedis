package com.github.wangshichun.jedis.internal;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by wangshichun on 2016/8/9.
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultSlaveSelectTest {
    private DefaultSlaveSelect slaveSelect = new DefaultSlaveSelect();

    @Test
    public void test() {
        List<JedisPoolWrapper> slaveList = new LinkedList<JedisPoolWrapper>();
        slaveList.add(new JedisPoolWrapper(null, "", 0));
        slaveList.add(new JedisPoolWrapper(null, "", 0));
        slaveList.add(new JedisPoolWrapper(null, "", 0));

        int size = slaveList.size();
        for (int i = 0; i < 10; i++) {
            Assert.assertTrue(null != slaveSelect.selectOneSlave(slaveList, null));

            slaveList = slaveSelect.selectAllPreferredSlave(slaveList, null);
            Assert.assertTrue(slaveList.size() == size);
        }
    }
}
