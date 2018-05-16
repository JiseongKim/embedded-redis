package redis.embedded;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.embedded.exceptions.EmbeddedRedisException;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

public class RedisCluster implements Redis {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final List<ClusterMaster> masters = new LinkedList();
    private final List<ClusterSlave> slaves = new LinkedList();

    private Integer waitForClusterTimeoutMS;
    private Integer waitForSetReplicaTimeoutMS;

    private String meetRedisIp = null;
    private Integer meetRedisPort = null;
    private String basicAuthPassword = null;

    RedisCluster(List<ClusterMaster> masters, List<ClusterSlave> slaves,
                 String meetRedisIp, Integer meetRedisPort) {
        this.masters.addAll(masters);
        this.slaves.addAll(slaves);
        this.meetRedisIp = meetRedisIp;
        this.meetRedisPort = meetRedisPort;
    }

    @Override
    public boolean isActive() {
        for(ClusterMaster master : masters) {
            if(!master.getMasterRedis().isActive()) {
                return false;
            }
        }
        for(ClusterSlave slave : slaves) {
            if(!slave.getSlaveRedis().isActive()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void start() throws EmbeddedRedisException {
        for (ClusterMaster master : masters) {
            master.getMasterRedis().start();

            if (meetRedisIp != null && meetRedisPort != null) {
                Jedis jedis = new Jedis(master.getMasterRedisIp(), master.getMasterRedisPort());
                if(this.getBasicAuthPassword() != null) {
                    jedis.auth(this.getBasicAuthPassword());
                }
                jedis.clusterMeet(meetRedisIp, meetRedisPort);
            }
        }
        allocSlotsToMaster();

        List<Future> replicaTasks = new LinkedList<>();
        ExecutorService executor = Executors.newFixedThreadPool(slaves.size());
        for (ClusterSlave slave : slaves) {
            slave.getSlaveRedis().start();

            final Jedis jedisSlave = new Jedis(slave.getSlaveIp(), slave.getSlavePort());

            if (this.getBasicAuthPassword() != null) {
                jedisSlave.auth(this.getBasicAuthPassword());
            }
            if (meetRedisIp != null && meetRedisPort != null) {
                jedisSlave.clusterMeet(meetRedisIp, meetRedisPort);
            }

            final Jedis jedisMaster = new Jedis(slave.getMasterRedisIp(), slave.getMasterRedisPort());

            if (this.getBasicAuthPassword() != null) {
                jedisMaster.auth(this.getBasicAuthPassword());
            }

            logger.info("start set replica");
            replicaTasks.add(executeSetReplica(executor, jedisMaster, jedisSlave));
            logger.info("end set replica");
        }

        for(Future future : replicaTasks) {
            try {
                future.get(getWaitForSetReplicaTimeoutMS(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException | InterruptedException | ExecutionException e) {
                logger.error(e.getMessage());
            }
        }

        executor.shutdown();
    }

    @Override
    public void stop() throws EmbeddedRedisException {
        for(ClusterMaster master : masters) {
            master.getMasterRedis().stop();
        }
        for(ClusterSlave slave : slaves) {
            slave.getSlaveRedis().stop();
        }
    }

    @Override
    public List<Integer> ports() {
        List<Integer> ports = new ArrayList<Integer>();
        ports.addAll(masterPorts());
        ports.addAll(slavesPorts());
        return ports;
    }

    public List<Integer> masterPorts() {
        List<Integer> ports = new ArrayList<Integer>();
        for(ClusterMaster master : masters) {
            ports.addAll(master.getMasterRedis().ports());
        }
        return ports;
    }

    public List<Integer> slavesPorts() {
        List<Integer> ports = new ArrayList<Integer>();
        for(ClusterSlave slave : slaves) {
            ports.addAll(slave.getSlaveRedis().ports());
        }
        return ports;
    }

    public static RedisClusterBuilder builder() {
        return new RedisClusterBuilder();
    }

    private void allocSlotsToMaster() {

        for(ClusterMaster master : masters) {

            int slotsPerNode = JedisCluster.HASHSLOTS / master.getClusterNodesCount();
            int remainedSlot = JedisCluster.HASHSLOTS % master.getClusterNodesCount();
            int additionalSlot = 0;

            int[] nodeSlots;
            if(master.getClusterNodesCount().equals(master.getIndexOfCluster() + 1)) {
                nodeSlots = new int[slotsPerNode + remainedSlot];
                additionalSlot = remainedSlot;
            } else {
                nodeSlots = new int[slotsPerNode];
            }

            Integer startSlot = master.getIndexOfCluster() * slotsPerNode;
            Integer endSlot = (master.getIndexOfCluster() + 1) * slotsPerNode -1 + additionalSlot;
            for (int i = startSlot, slot = 0 ; i <= endSlot; i++) {
                nodeSlots[slot++] = i;
            }

            Jedis jedis = new Jedis(master.getMasterRedisIp(), master.getMasterRedisPort());

            if(this.getBasicAuthPassword() != null) {
                jedis.auth(this.getBasicAuthPassword());
            }

            jedis.clusterAddSlots(nodeSlots);
        }
    }


    private Future executeSetReplica(ExecutorService executor, Jedis jedisMaster, Jedis jedisSlave) {

        class ClusterReplicaTask implements Runnable {
            private Jedis master;
            private Jedis slave;

            private ClusterReplicaTask(Jedis master, Jedis slave) {
                this.master = master;
                this.slave = slave;
            }

            public void run() {
                logger.info("executeSetReplica {}, {}",
                        master.getClient().getPort(),
                        slave.getClient().getPort());
                setReplica(master, slave);
            }
        }
        return executor.submit(new ClusterReplicaTask(jedisMaster, jedisSlave));
    }

    public void setReplica(Jedis master, Jedis slave) {

        try {
            waitForClusterReady(master);
        } catch (EmbeddedRedisException e) {
            logger.error(e.getMessage());
        }

        String masterSlave = "[master:" + master.getClient().getPort() + "/" +
                "slave:" +slave.getClient().getPort() + "]";

        for (String nodeInfo : master.clusterNodes().split("\n")) {
            if (nodeInfo.contains("myself")) {
                logger.debug("[setReplica][found]{}, {}",masterSlave, nodeInfo.split(" ")[0]);
                Boolean succeed = false;
                while(!succeed) {
                    try {
                        slave.clusterReplicate(nodeInfo.split(" ")[0]);
                    } catch (Exception e) {
                        String retryMessage =
                                String.format("trying to set replica %s->%s",
                                        slave.getClient().getPort(),
                                        master.getClient().getPort());
                        logger.info(retryMessage);
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException exSleep) {
                            logger.error(masterSlave + exSleep.getMessage());
                        }
                        continue;
                    }
                    succeed = true;
                }
                logger.info("[setReplica]Done {}", masterSlave);
            }
        }
    }

    public void waitForClusterReady(Jedis... nodes) throws EmbeddedRedisException {
        boolean clusterOk = false;

        long beginTime = System.currentTimeMillis();

        while (!clusterOk) {
            boolean isOk = true;
            for (Jedis node : nodes) {
                logger.debug("[waitForClusterReady]" + node.clusterInfo());
                if (!node.clusterInfo().split("\n")[0].contains("ok")) {
                    isOk = false;
                    break;
                }
            }

            if (isOk) {
                clusterOk = true;
            }

            long currentTime = System.currentTimeMillis();
            long elapsedTimeMS = currentTime - beginTime;

            if(getWaitForClusterTimeoutMS() != 0) {
                if (elapsedTimeMS > getWaitForClusterTimeoutMS()) {
                    throw new EmbeddedRedisException("Time out for waiting ready of cluster");
                }
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new EmbeddedRedisException("InterruptedException is raised", e);
            }
        }
    }

    public Integer getWaitForClusterTimeoutMS() {
        return waitForClusterTimeoutMS;
    }

    public RedisCluster setWaitForClusterTimeoutMS(Integer waitForClusterTimeoutMS) {
        this.waitForClusterTimeoutMS = waitForClusterTimeoutMS;
        return this;
    }

    public Integer getWaitForSetReplicaTimeoutMS() {
        return waitForSetReplicaTimeoutMS;
    }

    public void setWaitForSetReplicaTimeoutMS(Integer waitForSetReplicaTimeoutMS) {
        this.waitForSetReplicaTimeoutMS = waitForSetReplicaTimeoutMS;
    }

    public String getBasicAuthPassword() {
        return basicAuthPassword;
    }

    public void setBasicAuthPassword(String basicAuthPassword) {
        this.basicAuthPassword = basicAuthPassword;
    }
}
