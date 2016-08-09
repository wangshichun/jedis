package com.github.wangshichun.jedis.internal;

import com.github.wangshichun.jedis.ISlaveSelect;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 * Created by wangshichun on 2016/8/9.
 * @see com.github.wangshichun.jedis.ISlaveSelect
 * 随机选择策略
 */
public class DefaultSlaveSelect implements ISlaveSelect {
    private Random random = new Random();

    @Override
    public JedisPoolWrapper selectOneSlave(List<JedisPoolWrapper> slaveList, byte[] key) {
        if (slaveList != null && slaveList.size() > 0)
            return slaveList.get(random.nextInt(slaveList.size()));
        return null;
    }

    @Override
    public List<JedisPoolWrapper> selectAllPreferredSlave(List<JedisPoolWrapper> slaveList, byte[] key) {
        LinkedList<JedisPoolWrapper> resultList = new LinkedList<JedisPoolWrapper>();

        if (slaveList != null && slaveList.size() > 0) {
            slaveList = new LinkedList<JedisPoolWrapper>(slaveList);
            while (slaveList.size() > 0) {
                resultList.add(slaveList.remove(random.nextInt(slaveList.size())));
            }
        }
        return resultList;
    }
}
