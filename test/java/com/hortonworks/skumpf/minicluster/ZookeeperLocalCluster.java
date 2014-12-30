package com.hortonworks.skumpf.minicluster;

import org.apache.curator.test.TestingServer;

import java.io.IOException;

/**
 * In memory ZK cluster using Curator
 */
public class ZookeeperLocalCluster {

    private TestingServer zkTestServer;
    private int zkPort;

    public ZookeeperLocalCluster() {
        this(2181);
    }

    public ZookeeperLocalCluster(int zkPort) {
        this.zkPort = zkPort;
    }

    public void start() {
        System.out.println("ZOOKEEPER: Starting Zookeeper Instance On Port " + zkPort);
        try {
            zkTestServer = new TestingServer(zkPort);
        } catch(Exception e) {
            System.out.println("ERROR: Failed to start Zookeeper");
            e.getStackTrace();
            System.exit(1);
        }
        System.out.println("ZOOKEEPER: Zookeeper Instance " + getZkConnectionString() + " Successfully Started");
    }

    public void stop() throws IOException {
        System.out.println("ZOOKEEPER: Stopping Zookeeper Instance " + getZkConnectionString());
        zkTestServer.stop();
        System.out.println("ZOOKEEPER: Zookeeper Instance " + getZkConnectionString() + " Successfully Stopped");
    }

    public String getZkConnectionString() {
        return zkTestServer.getConnectString();
    }

    public String getZkHostName() {
        return getZkConnectionString().split(":")[0];
    }

    public String getZkPort() {
        return getZkConnectionString().split(":")[1];
    }

}
