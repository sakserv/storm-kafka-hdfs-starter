package com.hortonworks.skumpf.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import com.hortonworks.skumpf.minicluster.HdfsLocalCluster;
import com.hortonworks.skumpf.minicluster.KafkaLocalBroker;
import com.hortonworks.skumpf.minicluster.ZookeeperLocalCluster;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * Created by skumpf on 12/6/14.
 */
public class KafkaHdfsTopologyTest {

    // Kafka static
    private static final String DEFAULT_LOG_DIR = "/tmp/embedded/kafka/";
    private static final String TEST_TOPIC = "test-topic";
    private static final Integer KAFKA_PORT = 9092;
    private static final String LOCALHOST_BROKER = "localhost:" + KAFKA_PORT.toString();
    private static final Integer BROKER_ID = 1;

    // Storm static
    private static final String TEST_TOPOLOGY_NAME = "test";

    // Zookeeper
    private String zkHostsString;
    private ZookeeperLocalCluster zkCluster;

    // Kafka
    private KafkaLocalBroker kafkaCluster;

    // Storm
    private LocalCluster stormCluster;
    private TopologyBuilder builder = new TopologyBuilder();

    // HDFS
    private HdfsLocalCluster hdfsCluster;

    @Before
    public void setUp() {

        // Start ZK
        zkCluster = new ZookeeperLocalCluster();
        zkCluster.start();

        // Start Kafka
        kafkaCluster = new KafkaLocalBroker(TEST_TOPIC, DEFAULT_LOG_DIR, KAFKA_PORT, BROKER_ID, zkCluster.getZkConnectionString());
        kafkaCluster.start();

        // Start HDFS
        hdfsCluster = new HdfsLocalCluster();
        hdfsCluster.start();

        // Enable debug mode and start Storm
        stormCluster = new LocalCluster(zkCluster.getZkHostName(), Long.parseLong(zkCluster.getZkPort()));
    }

    @After
    public void tearDown() {

        // Stop Storm
        stormCluster.killTopology(TEST_TOPOLOGY_NAME);
        stormCluster.shutdown();

        // Stop Kafka
        kafkaCluster.stop();
        kafkaCluster.deleteOldTopics();

        // Stop HDFS
        hdfsCluster.stop();

        // Stop ZK
        zkCluster.stop();

    }

    @Test
    public void testKafkaHdfsTopology() {

        // Add Producer properties and created the Producer
        Properties props = new Properties();
        props.put("metadata.broker.list", LOCALHOST_BROKER);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);

        // Send 10 messages to the local kafka server:
        System.out.println("KAFKA: Preparing To Send 1000 Initial Messages");
        for (int i=0; i<10; i++){
            //KeyedMessage<String, String> data = new KeyedMessage<String, String>(TEST_TOPIC, StringUtils.repeat("test-message" + i, 20));
            KeyedMessage<String, String> data = new KeyedMessage<String, String>(TEST_TOPIC, "test-message" + i);
            producer.send(data);
            System.out.println("Sent message: " + data.toString());
        }
        System.out.println("KAFKA: Initial Messages Sent");

        // Stop the producer
        producer.close();

        // Topology
        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(3);

        System.out.println("STORM: Starting Topology: " + TEST_TOPOLOGY_NAME);
        TopologyBuilder builder = new TopologyBuilder();
        KafkaHdfsTopology.configureKafkaSpout(builder, zkCluster.getZkConnectionString(), TEST_TOPIC, "-2");
        KafkaHdfsTopology.configureHdfsBolt(builder, ",", "/tmp/kafka_data", hdfsCluster.getHdfsUriString());
        //MyPipeTopology.configurePrinterBolt(builder);
        stormCluster.submitTopology(TEST_TOPOLOGY_NAME, conf, builder.createTopology());

        try {
            Thread.sleep(10000L);
        } catch (InterruptedException e) {
            System.exit(1);
        }

        FileSystem hdfsFsHandle = hdfsCluster.getHdfsFileSystemHandle();
        try {
            RemoteIterator<LocatedFileStatus> listFiles = hdfsFsHandle.listFiles(new Path("/tmp/kafka_data"), true);
            while (listFiles.hasNext()) {
                LocatedFileStatus file = listFiles.next();

                System.out.println("HDFS READ: Found File: " + file);

                BufferedReader br = new BufferedReader(new InputStreamReader(hdfsFsHandle.open(file.getPath())));
                String line = br.readLine();
                while (line != null) {
                    System.out.println("HDFS READ: Found Line: " + line);
                    line = br.readLine();
                }
            }
            hdfsFsHandle.close();
        } catch(IOException e) {
            System.out.println(e);
        }
    }
}
