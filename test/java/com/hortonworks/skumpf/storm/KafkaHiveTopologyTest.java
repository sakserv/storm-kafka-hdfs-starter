package com.hortonworks.skumpf.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import com.hortonworks.skumpf.storm.tools.*;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.Properties;

/**
 * Created by skumpf on 12/6/14.
 */
public class KafkaHiveTopologyTest {

    // Kafka static
    private static final String DEFAULT_LOG_DIR = "/tmp/embedded/kafka/";
    private static final String TEST_TOPIC = "test-topic";
    private static final Integer KAFKA_PORT = 9092;
    private static final String LOCALHOST_BROKER = "localhost:" + KAFKA_PORT.toString();
    private static final Integer BROKER_ID = 1;

    // Storm static
    private static final String TEST_TOPOLOGY_NAME = "test";

    // Hive static
    private static final String HIVE_DB_NAME = "default";
    private static final String HIVE_TABLE_NAME = "test";

    // Zookeeper
    private String zkHostsString;
    private ZookeeperLocalCluster zkCluster;

    // Kafka
    private KafkaLocalBroker kafkaCluster;

    // Storm
    private LocalCluster stormCluster;
    private TopologyBuilder builder = new TopologyBuilder();

    // HDFS
    private HdfsCluster hdfsCluster;

    // Hive MetaStore
    private HiveLocalMetaStore hiveLocalMetaStore;

    // HiveServer2
    private HiveLocalServer hiveServer;

    @Before
    public void setUp() {

        // Start ZK
        zkCluster = new ZookeeperLocalCluster();
        zkCluster.start();

        // Start Kafka
        kafkaCluster = new KafkaLocalBroker(DEFAULT_LOG_DIR, KAFKA_PORT, BROKER_ID, zkCluster.getZkConnectionString());
        kafkaCluster.start();

        // Start HDFS
        hdfsCluster = new HdfsCluster();
        hdfsCluster.startHdfs();

        // Enable debug mode and start Storm
        stormCluster = new LocalCluster(zkCluster.getZkHostName(), Long.parseLong(zkCluster.getZkPort()));

        // Start HiveMetaStore
        hiveLocalMetaStore = new HiveLocalMetaStore();
        try {
            hiveLocalMetaStore.start();
        } catch(Exception e) {
            e.printStackTrace();
        }
        hiveLocalMetaStore.dumpMetaStoreConf();

        // Start HiveServer
        hiveServer = new HiveLocalServer(hiveLocalMetaStore.getMetaStoreUri());
        hiveServer.start();
    }

    @After
    public void tearDown() {

        // Stop HiveServer
        hiveServer.stop();

        // Stop HiveMetaStore
        try {
            hiveLocalMetaStore.stop();
        } catch(Exception e) {
            e.printStackTrace();
        }

        // Stop Storm
        stormCluster.killTopology(TEST_TOPOLOGY_NAME);
        stormCluster.shutdown();

        // Stop Kafka
        kafkaCluster.stop();
        kafkaCluster.deleteOldTopics();

        // Stop HDFS
        hdfsCluster.stopHdfs();

        // Stop ZK
        try {
            zkCluster.stop();
        } catch(IOException e) {
            System.out.println("ERROR: Failed to stop ZK... killing");
            System.exit(3);
        }
    }

    @Test
    public void testKafkaHiveTopology() {

        String[] partitionNames = {"dt"};

        String[] colNames = {"id", "msg"};

        // Load the Hive JDBC driver
        try {
            System.out.println("HIVE: Loading the Hive JDBC Driver");
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        } catch(ClassNotFoundException e) {
            e.printStackTrace();
        }


        // Establish a JDBC connection
        try {

            Connection con = DriverManager.getConnection("jdbc:hive2://localhost:" + hiveServer.getHiveServerThriftPort() + "/default", "user", "pass");

            String dropDdl = "DROP TABLE " + HIVE_DB_NAME + "." + HIVE_TABLE_NAME;

            Statement stmt = con.createStatement();
            System.out.println("HIVE: Running Create Table Statement: " + dropDdl);
            stmt.execute(dropDdl);

            String createDdl = "CREATE TABLE IF NOT EXISTS " + HIVE_DB_NAME + "." + HIVE_TABLE_NAME + " (id INT, msg STRING) " +
                "PARTITIONED BY (dt STRING) " +
                "CLUSTERED BY (id) INTO 16 BUCKETS " +
                "STORED AS ORC tblproperties(\"orc.compress\"=\"NONE\")";

            stmt = con.createStatement();
            System.out.println("HIVE: Running Create Table Statement: " + createDdl);
            stmt.execute(createDdl);

            System.out.println("HIVE: Validating Table was Created: ");
            ResultSet resultSet = stmt.executeQuery("DESCRIBE FORMATTED " + HIVE_TABLE_NAME);
            while (resultSet.next()) {
                System.out.println(resultSet.getString(1));
            }
        } catch(SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }



        // Add Producer properties and created the Producer
        Properties props = new Properties();
        props.put("metadata.broker.list", LOCALHOST_BROKER);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);

        // Send 10 messages to the local kafka server:
        System.out.println("KAFKA: Preparing To Send 1000 Initial Messages");
        for (int i=0; i<10; i++){

            // Create the JSON object
            JSONObject obj = new JSONObject();
            try {
                obj.put("id", String.valueOf(i));
                obj.put("msg", "test-message" + 1);
                obj.put("dt", GenerateRandomDay.genRandomDay());
            } catch(JSONException e) {
                e.printStackTrace();
                System.exit(3);
            }
            String payload = obj.toString();

            KeyedMessage<String, String> data = new KeyedMessage<String, String>(TEST_TOPIC, null, payload);
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
        KafkaHiveTopology.configureHiveStreamingBolt(builder, colNames, partitionNames, hiveLocalMetaStore.getMetaStoreUri(), HIVE_DB_NAME, HIVE_TABLE_NAME);
        //KafkaHdfsTopology.configureHdfsBolt(builder, ",", "/tmp/kafka_data", hdfsCluster.getHdfsUriString());
        //MyPipeTopology.configurePrinterBolt(builder);
        stormCluster.submitTopology(TEST_TOPOLOGY_NAME, conf, builder.createTopology());

        try {
            Thread.sleep(10000L);
        } catch (InterruptedException e) {
            System.exit(3);
        }

        //FileSystem hdfsFsHandle = hdfsCluster.getHdfsFileSystemHandle();
        //try {
        //    RemoteIterator<LocatedFileStatus> listFiles = hdfsFsHandle.listFiles(new Path("/tmp/kafka_data"), true);
        //    while (listFiles.hasNext()) {
        //        LocatedFileStatus file = listFiles.next();

        //        System.out.println("HDFS READ: Found File: " + file);

        //        BufferedReader br = new BufferedReader(new InputStreamReader(hdfsFsHandle.open(file.getPath())));
        //        String line = br.readLine();
        //        while (line != null) {
        //            System.out.println("HDFS READ: Found Line: " + line);
        //            line = br.readLine();
        //        }
        //    }
        //    hdfsFsHandle.close();
        //} catch(IOException e) {
        //    System.out.println(e);
        //}
    }
}
