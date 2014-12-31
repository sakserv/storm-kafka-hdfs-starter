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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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

    @Before
    public void setUp() {

        // Start ZK
        zkCluster = new ZookeeperLocalCluster();
        zkCluster.start();

        // Start HDFS
        hdfsCluster = new HdfsLocalCluster();
        hdfsCluster.start();

        // Start HiveMetaStore
        hiveLocalMetaStore = new HiveLocalMetaStore();
        hiveLocalMetaStore.start();

        // Start Kafka
        kafkaCluster = new KafkaLocalBroker(TEST_TOPIC, DEFAULT_LOG_DIR, KAFKA_PORT, BROKER_ID, zkCluster.getZkConnectionString());
        kafkaCluster.start();


        // Start Storm
        stormCluster = new StormLocalCluster(zkCluster.getZkHostName(), Long.parseLong(zkCluster.getZkPort()));
        stormCluster.start();

    }

    @After
    public void tearDown() {

        // Stop Storm
        stormCluster.stop(TEST_TOPOLOGY_NAME);

        // Stop Kafka
        kafkaCluster.stop();
        kafkaCluster.deleteOldTopics();

        // Stop HiveMetaStore
        hiveLocalMetaStore.stop();

        // Stop HDFS
        hdfsCluster.stop();

        // Stop ZK
        zkCluster.stop();

    }

    public void createTable() throws TException {
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
    }

    public void produceMessages() throws JSONException {
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
            obj.put("id", String.valueOf(i));
            obj.put("msg", "test-message" + 1);
            obj.put("dt", GenerateRandomDay.genRandomDay());
            String payload = obj.toString();

            KeyedMessage<String, String> data = new KeyedMessage<String, String>(TEST_TOPIC, null, payload);
            producer.send(data);
            System.out.println("Sent message: " + data.toString());
        }
        System.out.println("KAFKA: Initial Messages Sent");

        // Stop the producer
        producer.close();
    }

    public void runStormKafkaHiveTopology() {
        String[] partitionNames = {"dt"};
        String[] colNames = {"id", "msg"};
        System.out.println("STORM: Starting Topology: " + TEST_TOPOLOGY_NAME);
        TopologyBuilder builder = new TopologyBuilder();
        KafkaHiveTopology.configureKafkaSpout(builder, zkCluster.getZkConnectionString(), TEST_TOPIC, "-2");
        KafkaHiveTopology.configureHiveStreamingBolt(builder, colNames, partitionNames, hiveLocalMetaStore.getMetaStoreUri(), HIVE_DB_NAME, HIVE_TABLE_NAME);
        stormCluster.submitTopology(TEST_TOPOLOGY_NAME, new Config(), builder.createTopology());
    }

    @Test
    public void testKafkaHiveTopology() throws TException, JSONException {

        createTable();
        produceMessages();
        runStormKafkaHiveTopology();

        try {
            Thread.sleep(10000L);
        } catch (InterruptedException e) {
            System.exit(1);
        }

    }
}
