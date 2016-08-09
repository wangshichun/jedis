package com.github.wangshichun.jedis;

import com.github.wangshichun.jedis.internal.JedisPoolWrapper;

import java.util.List;

/**
 * Created by wangshichun on 2016/8/9.
 */
public interface ISlaveSelect {
    /**
     * @param slaveList 可用的从节点
     * @param key 要执行的命令的key，如果是多key的命令，此参数为第一个key
     * @return 返回一个slave，或者返回null以便只使用master
     * */
    JedisPoolWrapper selectOneSlave(List<JedisPoolWrapper> slaveList, byte[] key);
    /**
     * @param slaveList 可用的从节点
     * @param key 要执行的命令的key，如果是多key的命令，此参数为第一个key
     * @return 返回一个slave，或者返回null或空列表以便只使用master
     * */
    List<JedisPoolWrapper> selectAllPreferredSlave(List<JedisPoolWrapper> slaveList, byte[] key);
}
