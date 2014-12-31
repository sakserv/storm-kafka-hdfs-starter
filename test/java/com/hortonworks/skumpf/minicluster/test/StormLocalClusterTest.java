package com.hortonworks.skumpf.minicluster.test;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import com.hortonworks.skumpf.minicluster.StormLocalCluster;
import com.hortonworks.skumpf.minicluster.ZookeeperLocalCluster;
import com.hortonworks.skumpf.storm.KafkaHiveTopology;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by skumpf on 12/30/14.
 */
public class StormLocalClusterTest {

    ZookeeperLocalCluster zkCluster;
    StormLocalCluster stormCluster;

    static final String STORM_TEST_TOPOLOGY = "test";

    @Before
    public void setUp() {
        zkCluster = new ZookeeperLocalCluster();
        zkCluster.start();

        stormCluster = new StormLocalCluster(zkCluster.getZkHostName(), Long.parseLong(zkCluster.getZkPort()));
        stormCluster.start();
    }

    @After
    public void tearDown() {
        stormCluster.stop(STORM_TEST_TOPOLOGY);
        zkCluster.stop();
    }

    @Test
    public void testStormCluster() {
        TopologyBuilder builder = new TopologyBuilder();
        KafkaHiveTopology.configureRandomSentenceSpout(builder);
        KafkaHiveTopology.configurePrinterBolt(builder, "randomsentencespout");
        stormCluster.submitTopology(STORM_TEST_TOPOLOGY, new Config(), builder.createTopology());

        try {
            Thread.sleep(10000L);
        } catch (InterruptedException e) {
            System.out.println("SUCCESSFULLY COMPLETED");
        }
    }

}
