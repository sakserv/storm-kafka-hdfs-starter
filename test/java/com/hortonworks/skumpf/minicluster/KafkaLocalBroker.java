package com.hortonworks.skumpf.minicluster;

import com.hortonworks.skumpf.datetime.LocalSystemTime;
import com.hortonworks.skumpf.util.FileUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;

import java.io.File;
import java.util.Properties;

/**
 * In memory Kafka Broker for testing
 */

public class KafkaLocalBroker implements MiniCluster {

    //location of kafka logging file:
    public static final String DEFAULT_TEST_TOPIC = "test-topic";
    public static final String DEFAULT_LOG_DIR = "embedded_kafka";
    public static final int DEFAULT_PORT = 9092;
    public static final int DEFAULT_BROKER_ID = 1;
    public static final String DEFAULT_ZK_CONNECTION_STRING = "localhost:2181";

    public KafkaConfig conf;
    public KafkaServer server;

    private String topic;
    private String logDir;
    private int port;
    private int brokerId;
    private String zkConnString;

    /**
     * default constructor
     */
    public KafkaLocalBroker(){
        this(DEFAULT_TEST_TOPIC, DEFAULT_LOG_DIR, DEFAULT_PORT, DEFAULT_BROKER_ID, DEFAULT_ZK_CONNECTION_STRING);
    }

    public KafkaLocalBroker(String topic) {
        this(topic, DEFAULT_LOG_DIR, DEFAULT_PORT, DEFAULT_BROKER_ID, DEFAULT_ZK_CONNECTION_STRING);
    }

    public KafkaLocalBroker(String topic, String logDir, int port, int brokerId, String zkConnString){
        this.topic = topic;
        this.logDir = logDir;
        this.port = port;
        this.brokerId = brokerId;
        this.zkConnString = zkConnString;
        configure();
    }

    public void configure() {
        configure(logDir, port, brokerId, zkConnString);
    }

    public void configure(String logDir, int port, int brokerId, String zkConnString) {
        Properties properties = new Properties();
        properties.put("port", port+"");
        properties.put("broker.id", brokerId+"");
        properties.put("log.dir", logDir);
        properties.put("enable.zookeeper", "true");
        properties.put("zookeeper.connect", zkConnString);
        properties.put("advertised.host.name", "localhost");
        conf = new KafkaConfig(properties);
    }

    public void start() {
        server = new KafkaServer(conf, new LocalSystemTime());
        System.out.println("KAFKA: Starting Kafka on port: " + port);
        server.startup();
    }

    public void stop() {
        stop(false);
    }

    public void stop(boolean cleanUp){
        System.out.println("KAFKA: Stopping Kafka on port: " + port);
        server.shutdown();

        if (cleanUp) {
            System.out.println("KAFKA: Deleting Old Topics");
            deleteOldTopics(logDir);
        }
    }

    public void dumpConfig() {
        System.out.println("KAFKA CONFIG: " + conf.props().toString());
    }

    public int getPort() {
        return port;
    }

    public void deleteOldTopics(String dir) {
        //delete old Kafka topic files
        File delLogDir = new File(dir);
        if (delLogDir.exists()){
            FileUtils.deleteFolder(delLogDir.getAbsolutePath());
        }
    }
}