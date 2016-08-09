# jedis
Redis cluster client with reading and writing separation.

Jedis在Redis cluster发生master故障时，不会自动尝试slave；cluster中的slave只能作为备份，无法实现读写分离；因此导致故障时服务不可用、从节点的资源浪费等问题。


com.github.wangshichun.jedis.JedisCluster是从原来的jedis中的JedisCluster复制过来的，在原jedis的基础上少量修改，增加的功能：

1、读写分离。
<pre>
    调用JedisCluster中的setReadPreference方法，可设置多种策略。ReadPreference抽象类中定义的策略：
        MASTER_ONLY（默认），读操作连接master失败时不会自动重试slave。
        ONE_SLAVE_ONLY，读操作连接slave中的一个（默认随机选择slave，可通过调用JedisCluster中的
            setSlaveSelect方法实现自定义选择slave的策略）
        MASTER_THEN_ONE_SLAVE，先读master，连接失败选择一个slave
        MASTER_THEN_ALL_SLAVE，先读master，连接失败依次尝试所有slave
        ONE_SLAVE_THEN_MASTER，先读slave，连接失败选择master
        ALL_SLAVE_THEN_MASTER，依次尝试所有slave，连接都失败才会连接master
        可以继承ReadPreference抽象类，以根据slave列表、slot、key参数实现自己的策略。
</pre>

2、自定义slave选择策略，实现读本机房或近距离读。
<pre>
    使用读写分离功能时，可用自定义slave的选择策略。
    调用JedisCluster中的setSlaveSelect方法实现自定义选择slave的策略，根据slave列表、key计算
        可用的slave。返回的列表中，前面的连接会优先尝试。
</pre>

注意：我尚未在线上/生产环境实际使用。
