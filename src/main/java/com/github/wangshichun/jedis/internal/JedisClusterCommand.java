package com.github.wangshichun.jedis.internal;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.*;
import redis.clients.util.JedisClusterCRC16;
import redis.clients.util.SafeEncoder;

import java.util.List;
import java.util.NoSuchElementException;

/**
 * Created by wangshichun on 2016/8/8.
 */
public abstract class JedisClusterCommand<T> {

    private JedisSlotBasedConnectionHandler connectionHandler;
    private int redirections;
    private ThreadLocal<Jedis> askConnection = new ThreadLocal<Jedis>();

    public JedisClusterCommand(JedisSlotBasedConnectionHandler connectionHandler, int maxRedirections) {
        this.connectionHandler = connectionHandler;
        this.redirections = maxRedirections;
    }

    public abstract T execute(Jedis connection);

    public T run(boolean readOnly, String key) {
        if (key == null) {
            throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
        }

        return runWithRetries(SafeEncoder.encode(key), this.redirections, false, false, readOnly);
    }

    public T run(boolean readOnly, int keyCount, String... keys) {
        if (keys == null || keys.length == 0) {
            throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
        }

        // For multiple keys, only execute if they all share the
        // same connection slot.
        if (keys.length > 1) {
            int slot = JedisClusterCRC16.getSlot(keys[0]);
            for (int i = 1; i < keyCount; i++) {
                int nextSlot = JedisClusterCRC16.getSlot(keys[i]);
                if (slot != nextSlot) {
                    throw new JedisClusterException("No way to dispatch this command to Redis Cluster "
                            + "because keys have different slots.");
                }
            }
        }

        return runWithRetries(SafeEncoder.encode(keys[0]), this.redirections, false, false, readOnly);
    }

    public T runBinary(boolean readOnly, byte[] key) {
        if (key == null) {
            throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
        }

        return runWithRetries(key, this.redirections, false, false, readOnly);
    }

    public T runBinary(boolean readOnly, int keyCount, byte[]... keys) {
        if (keys == null || keys.length == 0) {
            throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
        }

        // For multiple keys, only execute if they all share the
        // same connection slot.
        if (keys.length > 1) {
            int slot = JedisClusterCRC16.getSlot(keys[0]);
            for (int i = 1; i < keyCount; i++) {
                int nextSlot = JedisClusterCRC16.getSlot(keys[i]);
                if (slot != nextSlot) {
                    throw new JedisClusterException("No way to dispatch this command to Redis Cluster "
                            + "because keys have different slots.");
                }
            }
        }

        return runWithRetries(keys[0], this.redirections, false, false, readOnly);
    }

    public T runWithAnyNode() {
        Jedis connection = null;
        try {
            connection = connectionHandler.getConnection();
            return execute(connection);
        } catch (JedisConnectionException e) {
            throw e;
        } finally {
            releaseConnection(connection);
        }
    }

    private T runWithRetries(byte[] key, int redirections, boolean tryRandomNode, boolean asking, boolean readOnly) {
        if (redirections <= 0) {
            throw new JedisClusterMaxRedirectionsException("Too many Cluster redirections?");
        }

        Jedis connection = null;
        try {

            if (asking) {
                // TODO: Pipeline asking with the original command to make it
                // faster....
                connection = askConnection.get();
                connection.asking();

                // if asking success, reset asking flag
                asking = false;
                return execute(connection);
            } else {
                if (tryRandomNode) {
                    connection = connectionHandler.getConnection();
                    return execute(connection);
                } else {
                    int slot = JedisClusterCRC16.getSlot(key);
                    if (readOnly) { // 只读命令，按策略选择主或从，或主从配合
                        List<JedisPool> poolList = connectionHandler.getReadPreferenceOrDefault().getJedisPool(connectionHandler, slot, key);
                        if (poolList == null || poolList.isEmpty()) {
                            throw new JedisClusterException(String.format("No connection available for slot: %s, key: %s", slot, new String(key)));
                        }
                        for (int i = 0; i < poolList.size(); i++) {
                            try {
                                connection = poolList.get(i).getResource();
                                return execute(connection);
                            } catch (JedisConnectionException connectionException) {
                                // ignore connectionException and try the next one
                            } catch (JedisException exception) {
                                if (exception != null && exception.getCause() instanceof NoSuchElementException) {
                                    // ignore NoSuchElementException(invoke getResource when jedisPool exhausted/timeout/unable to activate or validate) and try the next one
                                } else {
                                    throw exception;
                                }
                            } finally {
                                releaseConnection(connection);
                                connection = null;
                            }
                        }
                        throw new JedisClusterException(String.format("All connection seems to broken for slot: %s, key: %s", slot, new String(key)));
                    } else {
                        connection = connectionHandler.getConnectionFromSlot(slot);
                        return execute(connection);
                    }
                }
            }
        } catch (JedisConnectionException jce) {
            if (tryRandomNode) {
                // maybe all connection is down
                throw jce;
            }

            // release current connection before recursion
            releaseConnection(connection);
            connection = null;

            // retry with random connection
            return runWithRetries(key, redirections - 1, true, asking, readOnly);
        } catch (JedisRedirectionException jre) {
            // if MOVED redirection occurred,
            if (jre instanceof JedisMovedDataException) {
                // it rebuilds cluster's slot cache
                // recommended by Redis cluster specification
                this.connectionHandler.renewSlotCache(connection);
            }

            // release current connection before recursion or renewing
            releaseConnection(connection);
            connection = null;

            if (jre instanceof JedisAskDataException) {
                asking = true;
                askConnection.set(this.connectionHandler.getConnectionFromNode(jre.getTargetNode()));
            } else if (jre instanceof JedisMovedDataException) {
            } else {
                throw new JedisClusterException(jre);
            }

            return runWithRetries(key, redirections - 1, false, asking, readOnly);
        } finally {
            releaseConnection(connection);
        }
    }

    private void releaseConnection(Jedis connection) {
        if (connection != null) {
            connection.close();
        }
    }

}

