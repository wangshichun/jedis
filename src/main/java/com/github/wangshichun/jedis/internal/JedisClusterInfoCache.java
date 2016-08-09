package com.github.wangshichun.jedis.internal;

import redis.clients.jedis.Client;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.util.SafeEncoder;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * Created by wangshichun on 2016/8/8.
 */
public class JedisClusterInfoCache {
    private Map<String, JedisPool> nodes = new HashMap<String, JedisPool>();
    private Map<Integer, JedisPool> slots = new HashMap<Integer, JedisPool>();
    private Map<Integer, List<JedisPoolWrapper>> slaveSlots = new HashMap<Integer, List<JedisPoolWrapper>>();

    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private final Lock r = rwl.readLock();
    private final Lock w = rwl.writeLock();
    private final GenericObjectPoolConfig poolConfig;

    private int connectionTimeout;
    private int soTimeout;

    private static final int MASTER_NODE_INDEX = 2;

    public JedisClusterInfoCache(final GenericObjectPoolConfig poolConfig, int timeout) {
        this(poolConfig, timeout, timeout);
    }

    public JedisClusterInfoCache(final GenericObjectPoolConfig poolConfig,
                                 final int connectionTimeout, final int soTimeout) {
        this.poolConfig = poolConfig;
        this.connectionTimeout = connectionTimeout;
        this.soTimeout = soTimeout;
    }

    public void discoverClusterNodesAndSlots(Jedis jedis) {
        w.lock();

        try {
            Map<Integer, JedisPool> tmpSlots = new HashMap<Integer, JedisPool>();
            Map<Integer, List<JedisPoolWrapper>> tmpSlotsSlave = new HashMap<Integer, List<JedisPoolWrapper>>();
            Map<String, JedisPool> tmpNodes = new HashMap<String, JedisPool>();

            refreshNodesAndSlots(jedis, tmpSlots, tmpSlotsSlave, tmpNodes);
            this.slots = tmpSlots;
            this.slaveSlots = tmpSlotsSlave;
            this.nodes = tmpNodes;
        } finally {
            w.unlock();
        }
    }

    public void discoverClusterSlots(Jedis jedis) {
        w.lock();

        try {
            Map<Integer, JedisPool> tmpSlots = new HashMap<Integer, JedisPool>();
            Map<Integer, List<JedisPoolWrapper>> tmpSlotsSlave = new HashMap<Integer, List<JedisPoolWrapper>>();

            refreshNodesAndSlots(jedis, tmpSlots, tmpSlotsSlave, nodes);
            this.slots = tmpSlots;
            this.slaveSlots = tmpSlotsSlave;
        } finally {
            w.unlock();
        }
    }

    private void refreshNodesAndSlots(Jedis jedis, Map<Integer, JedisPool> slotsMaster, Map<Integer, List<JedisPoolWrapper>> slotsSlave, Map<String, JedisPool> nodes) {
        List<Object> slots = jedis.clusterSlots();

        for (Object slotInfoObj : slots) {
            List<Object> slotInfo = (List<Object>) slotInfoObj;

            if (slotInfo.size() <= MASTER_NODE_INDEX) {
                continue;
            }

            List<Integer> slotNums = getAssignedSlotArray(slotInfo);

            // hostInfos
            int size = slotInfo.size();
            for (int i = MASTER_NODE_INDEX; i < size; i++) {
                List<Object> hostInfos = (List<Object>) slotInfo.get(i);
                if (hostInfos.size() <= 0) {
                    continue;
                }

                HostAndPort targetNode = generateHostAndPort(hostInfos);
                setNodeIfNotExist(targetNode, nodes);
                if (i == MASTER_NODE_INDEX) {
                    assignSlotsToNode(slotNums, targetNode, nodes, slotsMaster);
                } else {
                    assignSlotsToNodeSlave(slotNums, targetNode, nodes, slotsSlave);
                }
            }
        }
    }

    private HostAndPort generateHostAndPort(List<Object> hostInfos) {
        return new HostAndPort(SafeEncoder.encode((byte[]) hostInfos.get(0)),
                ((Long) hostInfos.get(1)).intValue());
    }

    public void setNodeIfNotExist(HostAndPort node, Map<String, JedisPool> nodes) {
        w.lock();
        try {
            String nodeKey = getNodeKey(node);
            if (nodes == null)
                nodes = this.nodes;
            if (nodes.containsKey(nodeKey)) return;

            JedisPool nodePool = new JedisPool(poolConfig, node.getHost(), node.getPort(),
                    connectionTimeout, soTimeout, null, 0, null);
            nodes.put(nodeKey, nodePool);
        } finally {
            w.unlock();
        }
    }

    private void assignSlotsToNode(List<Integer> targetSlots, HostAndPort targetNode, Map<String, JedisPool> nodes, Map<Integer, JedisPool> slots) {
        w.lock();
        try {
            JedisPool targetPool = nodes.get(getNodeKey(targetNode));

            if (targetPool == null) {
                setNodeIfNotExist(targetNode, nodes);
                targetPool = nodes.get(getNodeKey(targetNode));
            }

            for (Integer slot : targetSlots) {
                slots.put(slot, targetPool);
            }
        } finally {
            w.unlock();
        }
    }

    private void assignSlotsToNodeSlave(List<Integer> targetSlots, HostAndPort targetNode, Map<String, JedisPool> nodes, Map<Integer, List<JedisPoolWrapper>> slots) {
        w.lock();
        try {
            JedisPool targetPool = nodes.get(getNodeKey(targetNode));

            if (targetPool == null) {
                setNodeIfNotExist(targetNode, nodes);
                targetPool = nodes.get(getNodeKey(targetNode));
            }

            for (Integer slot : targetSlots) {
                if (!slots.containsKey(slot)) {
                    slots.put(slot, new LinkedList<JedisPoolWrapper>());
                }
                slots.get(slot).add(new JedisPoolWrapper(targetPool, targetNode.getHost(), targetNode.getPort()));
            }
        } finally {
            w.unlock();
        }
    }

    public JedisPool getNode(String nodeKey) {
        r.lock();
        try {
            return nodes.get(nodeKey);
        } finally {
            r.unlock();
        }
    }

    public JedisPool getSlotPool(int slot) {
        r.lock();
        try {
            return slots.get(slot);
        } finally {
            r.unlock();
        }
    }

    public List<JedisPoolWrapper> getSlotPoolSlave(int slot) {
        r.lock();
        try {
            return slaveSlots.get(slot);
        } finally {
            r.unlock();
        }
    }

    public Map<String, JedisPool> getNodes() {
        r.lock();
        try {
            return new HashMap<String, JedisPool>(nodes);
        } finally {
            r.unlock();
        }
    }

    public static String getNodeKey(HostAndPort hnp) {
        return hnp.getHost() + ":" + hnp.getPort();
    }

    public static String getNodeKey(Client client) {
        return client.getHost() + ":" + client.getPort();
    }

    public static String getNodeKey(Jedis jedis) {
        return getNodeKey(jedis.getClient());
    }

    private List<Integer> getAssignedSlotArray(List<Object> slotInfo) {
        List<Integer> slotNums = new ArrayList<Integer>();
        for (int slot = ((Long) slotInfo.get(0)).intValue(); slot <= ((Long) slotInfo.get(1))
                .intValue(); slot++) {
            slotNums.add(slot);
        }
        return slotNums;
    }

}
