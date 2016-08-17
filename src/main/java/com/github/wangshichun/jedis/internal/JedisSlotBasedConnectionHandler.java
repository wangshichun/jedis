package com.github.wangshichun.jedis.internal;

import com.github.wangshichun.jedis.ISlaveSelect;
import com.github.wangshichun.jedis.ReadPreference;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

import java.util.*;

/**
 * Created by wangshichun on 2016/8/8.
 */
public class JedisSlotBasedConnectionHandler {
    protected final com.github.wangshichun.jedis.internal.JedisClusterInfoCache cache;

    public JedisSlotBasedConnectionHandler(Set<HostAndPort> nodes,
                                           final GenericObjectPoolConfig poolConfig, int timeout) {
        this(nodes, poolConfig, timeout, timeout, null);
    }

    public JedisSlotBasedConnectionHandler(Set<HostAndPort> nodes,
                                           final GenericObjectPoolConfig poolConfig, int connectionTimeout, int soTimeout, String password) {
        this.cache = new JedisClusterInfoCache(poolConfig, connectionTimeout, soTimeout, password);
        initializeSlotsCache(nodes, poolConfig, password);
    }

    private ReadPreference readPreference;
    private ISlaveSelect slaveSelect;
    private final DefaultSlaveSelect DEFAULT_SLAVE_SELECT = new DefaultSlaveSelect();

    public void setReadPreference(ReadPreference readPreference) {
        this.readPreference = readPreference;
    }

    public ReadPreference getReadPreference() {
        return this.readPreference;
    }
    public ReadPreference getReadPreferenceOrDefault() {
        if (readPreference == null) {
            return ReadPreference.MASTER_ONLY;
        }
        return readPreference;
    }

    public ISlaveSelect getSlaveSelect() {
        return slaveSelect;
    }
    public ISlaveSelect getSlaveSelectOrDefault() {
        if (slaveSelect == null) {
            return DEFAULT_SLAVE_SELECT;
        }
        return slaveSelect;
    }

    public void setSlaveSelect(ISlaveSelect slaveSelect) {
        this.slaveSelect = slaveSelect;
    }

    public Jedis getConnection() {
        // In antirez's redis-rb-cluster implementation,
        // getRandomConnection always return valid connection (able to
        // ping-pong)
        // or exception if all connections are invalid

        List<JedisPool> pools = getShuffledNodesPool();

        for (JedisPool pool : pools) {
            Jedis jedis = null;
            try {
                jedis = pool.getResource();

                if (jedis == null) {
                    continue;
                }

                String result = jedis.ping();
                if (result.equalsIgnoreCase("pong"))
                    return jedis;
                jedis.close();
            } catch (JedisException ex) {
                if (jedis != null) {
                    jedis.close();
                }
            }
        }

        throw new JedisConnectionException("no reachable node in cluster");
    }

    public Jedis getConnectionFromSlot(int slot) {
        JedisPool connectionPool = cache.getSlotPool(slot);
        if (connectionPool != null) {
            // It can't guaranteed to get valid connection because of node
            // assignment
            return connectionPool.getResource();
        } else {
            return getConnection();
        }
    }

    public JedisPool getMasterPoolFromSlot(int slot) {
        JedisPool connectionPool = cache.getSlotPool(slot);
        return connectionPool;
    }

    public List<JedisPoolWrapper> getSlavePoolFromSlot(int slot) {
        List<JedisPoolWrapper> connectionPoolList = cache.getSlotPoolSlave(slot);
        if (connectionPoolList == null)
            return new LinkedList<JedisPoolWrapper>();
        return Collections.unmodifiableList(connectionPoolList);
    }

    public Jedis getConnectionFromNode(HostAndPort node) {
        cache.setNodeIfNotExist(node, null);
        return cache.getNode(JedisClusterInfoCache.getNodeKey(node)).getResource();
    }

    public Map<String, JedisPool> getNodes() {
        return cache.getNodes();
    }

    private void initializeSlotsCache(Set<HostAndPort> startNodes, GenericObjectPoolConfig poolConfig, String password) {
        for (HostAndPort hostAndPort : startNodes) {
            Jedis jedis = new Jedis(hostAndPort.getHost(), hostAndPort.getPort());
            try {
                if (password != null)
                    jedis.auth(password);
                cache.discoverClusterNodesAndSlots(jedis);
                break;
            } catch (JedisConnectionException e) {
                // try next nodes
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
        }

        for (HostAndPort node : startNodes) {
            cache.setNodeIfNotExist(node, null);
        }
    }

    public void renewSlotCache() {
        for (JedisPool jp : getShuffledNodesPool()) {
            Jedis jedis = null;
            try {
                jedis = jp.getResource();
                cache.discoverClusterSlots(jedis);
                break;
            } catch (JedisConnectionException e) {
                // try next nodes
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
        }
    }

    public void renewSlotCache(Jedis jedis) {
        try {
            cache.discoverClusterSlots(jedis);
        } catch (JedisConnectionException e) {
            renewSlotCache();
        }
    }

    /**获取随机的列表*/
    protected List<JedisPool> getShuffledNodesPool() {
        List<JedisPool> pools = new ArrayList<JedisPool>();
        pools.addAll(cache.getNodes().values());
        Collections.shuffle(pools);
        return pools;
    }
}
