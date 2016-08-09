package com.github.wangshichun.jedis;

import com.github.wangshichun.jedis.internal.DefaultSlaveSelect;
import com.github.wangshichun.jedis.internal.JedisPoolWrapper;
import com.github.wangshichun.jedis.internal.JedisSlotBasedConnectionHandler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import redis.clients.jedis.JedisPool;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by wangshichun on 2016/8/9.
 */
@RunWith(MockitoJUnitRunner.class)
public class ReadPreferenceTest {
    @Mock
    private JedisSlotBasedConnectionHandler connectionHandler;

    @Before
    public void setUp() {
        Mockito.when(connectionHandler.getSlaveSelectOrDefault())
                .thenReturn(new DefaultSlaveSelect());
    }

    private void setUpNullMaster() {
        Mockito.when(connectionHandler.getMasterPoolFromSlot(Mockito.anyInt()))
                .thenReturn(null);
    }
    private void setUpNotNullMaster() {
        Mockito.when(connectionHandler.getMasterPoolFromSlot(Mockito.anyInt()))
                .thenReturn(new TestJedisPool("master"));
    }
    private void setUpNullSlave() {
        Mockito.when(connectionHandler.getSlavePoolFromSlot(Mockito.anyInt()))
                .thenReturn(null);
    }
    private int setUpNotNullSlave() {
        List<JedisPoolWrapper> wrapperList = new LinkedList<JedisPoolWrapper>();
        wrapperList.add(new JedisPoolWrapper(new TestJedisPool("slave1"), "", 0));
        wrapperList.add(new JedisPoolWrapper(new TestJedisPool("slave2"), "", 0));
        wrapperList.add(new JedisPoolWrapper(new TestJedisPool("slave3"), "", 0));
        Mockito.when(connectionHandler.getSlavePoolFromSlot(Mockito.anyInt()))
                .thenReturn(wrapperList);
        return wrapperList.size();
    }

    @Test
    public void testMasterOnly() {
        setUpNullMaster();
        List<JedisPool> poolList = ReadPreference.MASTER_ONLY.getJedisPool(connectionHandler, 0, null);
        Assert.assertTrue(poolList == null || poolList.isEmpty());

        setUpNotNullMaster();
        poolList = ReadPreference.MASTER_ONLY.getJedisPool(connectionHandler, 0, null);
        Assert.assertTrue(poolList != null && poolList.size() == 1);
        Assert.assertTrue(((TestJedisPool) poolList.get(0)).description.contains("master"));
    }

    @Test
    public void testOneSlaveOnly() {
        setUpNullSlave();
        List<JedisPool> poolList = ReadPreference.ONE_SLAVE_ONLY.getJedisPool(connectionHandler, 0, null);
        Assert.assertTrue(poolList == null || poolList.isEmpty());

        setUpNotNullSlave();
        poolList = ReadPreference.ONE_SLAVE_ONLY.getJedisPool(connectionHandler, 0, null);
        Assert.assertTrue(poolList != null && poolList.size() == 1);
        Assert.assertTrue(((TestJedisPool) poolList.get(0)).description.contains("slave"));
    }

    @Test
    public void testMasterThenOneSlave() {
        setUpNullMaster();
        setUpNullSlave();
        List<JedisPool> poolList = ReadPreference.MASTER_THEN_ONE_SLAVE.getJedisPool(connectionHandler, 0, null);
        Assert.assertTrue(poolList == null || poolList.isEmpty());

        setUpNotNullMaster();
        poolList = ReadPreference.MASTER_THEN_ONE_SLAVE.getJedisPool(connectionHandler, 0, null);
        Assert.assertTrue(poolList != null && poolList.size() == 1);
        Assert.assertTrue(((TestJedisPool) poolList.get(0)).description.contains("master"));

        setUpNotNullSlave();
        poolList = ReadPreference.MASTER_THEN_ONE_SLAVE.getJedisPool(connectionHandler, 0, null);
        Assert.assertTrue(poolList != null && poolList.size() == 1 + 1);
        Assert.assertTrue(((TestJedisPool) poolList.get(0)).description.contains("master"));
        Assert.assertTrue(((TestJedisPool) poolList.get(1)).description.contains("slave"));
    }

    @Test
    public void testMasterThenAllSlave() {
        setUpNullMaster();
        setUpNullSlave();
        List<JedisPool> poolList = ReadPreference.MASTER_THEN_ALL_SLAVE.getJedisPool(connectionHandler, 0, null);
        Assert.assertTrue(poolList == null || poolList.isEmpty());

        setUpNotNullMaster();
        poolList = ReadPreference.MASTER_THEN_ALL_SLAVE.getJedisPool(connectionHandler, 0, null);
        Assert.assertTrue(poolList != null && poolList.size() == 1);
        Assert.assertTrue(((TestJedisPool) poolList.get(0)).description.contains("master"));

        int size = setUpNotNullSlave();
        poolList = ReadPreference.MASTER_THEN_ALL_SLAVE.getJedisPool(connectionHandler, 0, null);
        Assert.assertTrue(poolList != null && poolList.size() == 1 + size);
        Assert.assertTrue(((TestJedisPool) poolList.get(0)).description.contains("master"));
        Assert.assertTrue(((TestJedisPool) poolList.get(1)).description.contains("slave"));
    }

    @Test
    public void testOneSlaveThenMaster() {
        setUpNullMaster();
        setUpNullSlave();
        List<JedisPool> poolList = ReadPreference.ONE_SLAVE_THEN_MASTER.getJedisPool(connectionHandler, 0, null);
        Assert.assertTrue(poolList == null || poolList.isEmpty());

        setUpNotNullMaster();
        poolList = ReadPreference.ONE_SLAVE_THEN_MASTER.getJedisPool(connectionHandler, 0, null);
        Assert.assertTrue(poolList != null && poolList.size() == 1);
        Assert.assertTrue(((TestJedisPool) poolList.get(0)).description.contains("master"));

        setUpNotNullSlave();
        poolList = ReadPreference.ONE_SLAVE_THEN_MASTER.getJedisPool(connectionHandler, 0, null);
        Assert.assertTrue(poolList != null && poolList.size() == 1 + 1);
        Assert.assertTrue(((TestJedisPool) poolList.get(1)).description.contains("master"));
        Assert.assertTrue(((TestJedisPool) poolList.get(0)).description.contains("slave"));
    }

    @Test
    public void testAllSlaveThenMaster() {
        setUpNullMaster();
        setUpNullSlave();
        List<JedisPool> poolList = ReadPreference.ALL_SLAVE_THEN_MASTER.getJedisPool(connectionHandler, 0, null);
        Assert.assertTrue(poolList == null || poolList.isEmpty());

        setUpNotNullMaster();
        poolList = ReadPreference.ALL_SLAVE_THEN_MASTER.getJedisPool(connectionHandler, 0, null);
        Assert.assertTrue(poolList != null && poolList.size() == 1);
        Assert.assertTrue(((TestJedisPool) poolList.get(0)).description.contains("master"));

        int size = setUpNotNullSlave();
        poolList = ReadPreference.ALL_SLAVE_THEN_MASTER.getJedisPool(connectionHandler, 0, null);
        Assert.assertTrue(poolList != null && poolList.size() == 1 + size);
        Assert.assertTrue(((TestJedisPool) poolList.get(0)).description.contains("slave"));
        Assert.assertTrue(((TestJedisPool) poolList.get(size)).description.contains("master"));
    }

    private class  TestJedisPool extends JedisPool {
        private String description;
        public TestJedisPool(String description) {
            this.description = description;
        }
    }
}
