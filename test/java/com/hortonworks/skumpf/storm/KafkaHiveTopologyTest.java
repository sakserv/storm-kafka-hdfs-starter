package com.hortonworks.skumpf.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import com.hortonworks.skumpf.datetime.GenerateRandomDay;
import com.hortonworks.skumpf.minicluster.*;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import net.hydromatic.optiq.*;
import net.hydromatic.optiq.Schema;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.thrift.TException;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

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
    private StormLocalCluster stormCluster;
    private TopologyBuilder builder = new TopologyBuilder();

    // HDFS
    private HdfsLocalCluster hdfsCluster;

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
        hdfsCluster = new HdfsLocalCluster();
        hdfsCluster.start();

        // Enable debug mode and start Storm
        stormCluster = new StormLocalCluster(zkCluster.getZkHostName(), Long.parseLong(zkCluster.getZkPort()));

        // Start HiveMetaStore
        hiveLocalMetaStore = new HiveLocalMetaStore();
        try {
            hiveLocalMetaStore.start();
        } catch(Exception e) {
            e.printStackTrace();
        }
        //hiveLocalMetaStore.dumpMetaStoreConf();

    }

    @After
    public void tearDown() {

        // Stop HiveServer
        //hiveServer.stop();

        // Stop HiveMetaStore
        try {
            hiveLocalMetaStore.stop();
        } catch(Exception e) {
            e.printStackTrace();
        }

        // Stop Storm
        stormCluster.stop(TEST_TOPOLOGY_NAME);

        // Stop Kafka
        kafkaCluster.stop();
        kafkaCluster.deleteOldTopics();

        // Stop HDFS
        hdfsCluster.stop();

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
        //try {
        //    System.out.println("HIVE: Loading the Hive JDBC Driver");
        //    Class.forName("org.apache.hive.jdbc.HiveDriver");
        //} catch(ClassNotFoundException e) {
        //    e.printStackTrace();
        //}


        try {
            HiveMetaStoreClient hiveClient = new HiveMetaStoreClient(hiveLocalMetaStore.getConf());

            hiveClient.dropTable(HIVE_DB_NAME, HIVE_TABLE_NAME, true, true);

            // Define the cols
            List<FieldSchema> cols = new ArrayList<FieldSchema>();
            cols.add(new FieldSchema("id", Constants.INT_TYPE_NAME, ""));
            cols.add(new FieldSchema("msg", Constants.STRING_TYPE_NAME, ""));

            // Values for the StorageDescriptor
            String location = "/tmp/test_table";
            String inputFormat = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
            String outputFormat = "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat";
            int numBuckets = 16;
            Map<String,String> orcProps = new HashMap<String, String>();
            orcProps.put("orc.compress", "NONE");
            SerDeInfo serDeInfo = new SerDeInfo(OrcSerde.class.getSimpleName(), OrcSerde.class.getName(), orcProps);
            List<String> bucketCols = new ArrayList<String>();
            bucketCols.add("id");

            // Build the StorageDescriptor
            StorageDescriptor sd = new StorageDescriptor();
            sd.setCols(cols);
            sd.setLocation(location);
            sd.setInputFormat(inputFormat);
            sd.setOutputFormat(outputFormat);
            sd.setNumBuckets(numBuckets);
            sd.setSerdeInfo(serDeInfo);
            sd.setBucketCols(bucketCols);
            sd.setSortCols(new ArrayList<Order>());
            sd.setParameters(new HashMap<String, String>());

            // Define the table
            Table tbl = new Table();
            tbl.setDbName(HIVE_DB_NAME);
            tbl.setTableName(HIVE_TABLE_NAME);
            tbl.setSd(sd);
            tbl.setOwner(System.getProperty("user.name"));
            tbl.setParameters(new HashMap<String, String>());
            tbl.setViewOriginalText("");
            tbl.setViewExpandedText("");
            tbl.setTableType(TableType.EXTERNAL_TABLE.name());
            List<FieldSchema> partitions = new ArrayList<FieldSchema>();
            partitions.add(new FieldSchema("dt", Constants.STRING_TYPE_NAME, ""));
            tbl.setPartitionKeys(partitions);

            // Create the table
            hiveClient.createTable(tbl);

            // Describe the table
            Table createdTable = hiveClient.getTable(HIVE_DB_NAME, HIVE_TABLE_NAME);
            System.out.println("HIVE: Created Table: " + createdTable.toString());

        } catch(MetaException e) {
            e.printStackTrace();
        } catch(TException e) {
            e.printStackTrace();
        }

        // Establish a JDBC connection
        //try {

        //    Connection con = DriverManager.getConnection("jdbc:hive2://localhost:" + hiveServer.getHiveServerThriftPort() + "/default", "user", "pass");

        //    String dropDdl = "DROP TABLE " + HIVE_DB_NAME + "." + HIVE_TABLE_NAME;

        //    Statement stmt = con.createStatement();
        //    System.out.println("HIVE: Running Drop Table Statement: " + dropDdl);
        //    stmt.execute(dropDdl);

        //    String createDdl = "CREATE TABLE IF NOT EXISTS " + HIVE_DB_NAME + "." + HIVE_TABLE_NAME + " (id INT, msg STRING) " +
        //        "PARTITIONED BY (dt STRING) " +
        //        "CLUSTERED BY (id) INTO 16 BUCKETS " +
        //        "STORED AS ORC tblproperties(\"orc.compress\"=\"NONE\")";

        //    stmt = con.createStatement();
        //    System.out.println("HIVE: Running Create Table Statement: " + createDdl);
        //    stmt.execute(createDdl);

        //    System.out.println("HIVE: Validating Table was Created: ");
        //    ResultSet resultSet = stmt.executeQuery("DESCRIBE FORMATTED " + HIVE_TABLE_NAME);
        //    while (resultSet.next()) {
        //        System.out.println(resultSet.getString(1));
        //    }
        //} catch(SQLException e) {
        //    e.printStackTrace();
        //    System.exit(1);
        //}



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
        KafkaHiveTopology.configureKafkaSpout(builder, zkCluster.getZkConnectionString(), TEST_TOPIC, "-2");
        KafkaHiveTopology.configureHiveStreamingBolt(builder, colNames, partitionNames, hiveLocalMetaStore.getMetaStoreUri(), HIVE_DB_NAME, HIVE_TABLE_NAME);
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
