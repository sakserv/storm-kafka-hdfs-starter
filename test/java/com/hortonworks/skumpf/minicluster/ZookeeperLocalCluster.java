package com.hortonworks.skumpf.minicluster;

import org.apache.commons.math.stat.descriptive.rank.Min;
import org.apache.curator.test.TestingServer;

import java.io.IOException;

/**
 * In memory ZK cluster using Curator
 */
public class ZookeeperLocalCluster implements MiniCluster {

    private TestingServer zkTestServer;
    private int zkPort;

    public ZookeeperLocalCluster() {
        configure();
    }

    public ZookeeperLocalCluster(int zkPort) {
        configure(zkPort);
    }

    public void configure() {
        zkPort = 2181;
        configure(zkPort);
    }

    public void configure(int zkPort) {
        this.zkPort = zkPort;
    }

    public void start() {
        System.out.println("ZOOKEEPER: Starting Zookeeper Instance On Port " + zkPort);
        try {
            zkTestServer = new TestingServer(zkPort);
        } catch(Exception e) {
            System.out.println("ERROR: Failed to start Zookeeper");
            e.getStackTrace();
        }
        System.out.println("ZOOKEEPER: Zookeeper Instance " + getZkConnectionString() + " Successfully Started");
    }

    public void stop()  {
        System.out.println("ZOOKEEPER: Stopping Zookeeper Instance " + getZkConnectionString());
        try {
            zkTestServer.stop();
        } catch(IOException e) {
            e.printStackTrace();
        }
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

    public void dumpConfig() {
        System.out.println("ZOOKEEPER CONFIG: " + zkTestServer.getTempDirectory());
    }

}
