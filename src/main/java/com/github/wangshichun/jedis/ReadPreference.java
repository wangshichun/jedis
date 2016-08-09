package com.github.wangshichun.jedis;

import com.github.wangshichun.jedis.internal.JedisPoolWrapper;
import com.github.wangshichun.jedis.internal.JedisSlotBasedConnectionHandler;
import redis.clients.jedis.JedisPool;

import java.util.*;

/**
 * Created by wangshichun on 2016/8/9.
 */
public abstract class ReadPreference {
    public abstract List<JedisPool> getJedisPool(JedisSlotBasedConnectionHandler connectionHandler, int slot, byte[] key);

    public static final ReadPreference ALL_SLAVE_THEN_MASTER = new ReadPreference() {
        @Override
        public List<JedisPool> getJedisPool(JedisSlotBasedConnectionHandler connectionHandler, int slot, byte[] key) {
            LinkedList<JedisPool> resultList = new LinkedList<JedisPool>();

            List<JedisPoolWrapper> list = connectionHandler.getSlavePoolFromSlot(slot);
            ISlaveSelect slaveSelect = connectionHandler.getSlaveSelectOrDefault();
            list = slaveSelect.selectAllPreferredSlave(list, key);
            if (list != null) {
                for (JedisPoolWrapper wrapper : list) {
                    resultList.add(wrapper.getJedisPool());
                }
            }
            JedisPool jedisPool = connectionHandler.getMasterPoolFromSlot(slot);
            if (jedisPool != null) {
                resultList.add(jedisPool);
            }
            return resultList;
        }
    };

    public static final ReadPreference ONE_SLAVE_THEN_MASTER = new ReadPreference() {
        @Override
        public List<JedisPool> getJedisPool(JedisSlotBasedConnectionHandler connectionHandler, int slot, byte[] key) {
            LinkedList<JedisPool> resultList = new LinkedList<JedisPool>();
            List<JedisPoolWrapper> list = connectionHandler.getSlavePoolFromSlot(slot);
            if (list != null && list.size() > 0) {
                ISlaveSelect slaveSelect = connectionHandler.getSlaveSelectOrDefault();
                JedisPoolWrapper wrapper = slaveSelect.selectOneSlave(list, key);
                if (wrapper != null)
                    resultList.add(wrapper.getJedisPool());
            }
            JedisPool jedisPool = connectionHandler.getMasterPoolFromSlot(slot);
            if (jedisPool != null) {
                resultList.add(jedisPool);
            }
            return resultList;
        }
    };

    public static final ReadPreference MASTER_THEN_ALL_SLAVE = new ReadPreference() {
        @Override
        public List<JedisPool> getJedisPool(JedisSlotBasedConnectionHandler connectionHandler, int slot, byte[] key) {
            JedisPool jedisPool = connectionHandler.getMasterPoolFromSlot(slot);
            LinkedList<JedisPool> resultList = new LinkedList<JedisPool>();
            if (jedisPool != null) {
                resultList.add(jedisPool);
            }
            List<JedisPoolWrapper> list = connectionHandler.getSlavePoolFromSlot(slot);
            if (list != null && list.size() > 0) {
                ISlaveSelect slaveSelect = connectionHandler.getSlaveSelectOrDefault();
                list = slaveSelect.selectAllPreferredSlave(list, key);
                if (list != null && list.size() > 0) {
                    for (JedisPoolWrapper wrapper : list) {
                        resultList.add(wrapper.getJedisPool());
                    }
                }
            }
            return resultList;
        }
    };

    public static final ReadPreference MASTER_THEN_ONE_SLAVE = new ReadPreference() {
        @Override
        public List<JedisPool> getJedisPool(JedisSlotBasedConnectionHandler connectionHandler, int slot, byte[] key) {
            JedisPool jedisPool = connectionHandler.getMasterPoolFromSlot(slot);
            LinkedList<JedisPool> resultList = new LinkedList<JedisPool>();
            if (jedisPool != null) {
                resultList.add(jedisPool);
            }
            List<JedisPoolWrapper> list = connectionHandler.getSlavePoolFromSlot(slot);
            if (list != null && list.size() > 0) {
                ISlaveSelect slaveSelect = connectionHandler.getSlaveSelectOrDefault();
                JedisPoolWrapper wrapper = slaveSelect.selectOneSlave(list, key);
                if (wrapper != null)
                    resultList.add(wrapper.getJedisPool());
            }
            return resultList;
        }
    };

    public static final ReadPreference ONE_SLAVE_ONLY = new ReadPreference() {
        @Override
        public List<JedisPool> getJedisPool(JedisSlotBasedConnectionHandler connectionHandler, int slot, byte[] key) {
            List<JedisPoolWrapper> list = connectionHandler.getSlavePoolFromSlot(slot);
            if (list != null && list.size() > 0) {
                ISlaveSelect slaveSelect = connectionHandler.getSlaveSelectOrDefault();
                JedisPoolWrapper wrapper = slaveSelect.selectOneSlave(list, key);
                if (wrapper != null)
                    return Collections.singletonList(wrapper.getJedisPool());
            }
            return null;
        }
    };

    public static final ReadPreference MASTER_ONLY = new ReadPreference() {
        @Override
        public List<JedisPool> getJedisPool(JedisSlotBasedConnectionHandler connectionHandler, int slot, byte[] key) {
            JedisPool jedisPool = connectionHandler.getMasterPoolFromSlot(slot);
            if (jedisPool != null)
                return Collections.singletonList(jedisPool);
            return null;
        }
    };

    public static void main(String[] args) {
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            System.out.println(random.nextInt(3));
        }
    }
}
