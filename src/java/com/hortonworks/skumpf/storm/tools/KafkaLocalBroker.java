package com.hortonworks.skumpf.storm.tools;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;

import java.io.File;
import java.util.Properties;

/**
 * In memory Kafka Broker for testing
 */

public class KafkaLocalBroker {

    //location of kafka logging file:
    public static final String DEFAULT_LOG_DIR = "/tmp/embedded/kafka/";
    public static final String TEST_TOPIC = "test-topic";

    public KafkaConfig kafkaConfig;
    //This is the actual Kafka server:
    public KafkaServer kafkaServer;

    /**
     * default constructor
     */
    public KafkaLocalBroker(){
        this(DEFAULT_LOG_DIR, 9092, 1, "localhost:2181");
    }

    public KafkaLocalBroker(Properties properties){
        kafkaConfig = new KafkaConfig(properties);
        kafkaServer = new KafkaServer(kafkaConfig, new LocalSystemTime());
    }

    public KafkaLocalBroker(String logDir, int port, int brokerId, String zkConnString){
        this(createProperties(logDir, port, brokerId, zkConnString));
    }

    private static Properties createProperties(String logDir, int port, int brokerId, String zkConnString) {
        Properties properties = new Properties();
        properties.put("port", port+"");
        properties.put("broker.id", brokerId+"");
        properties.put("log.dir", logDir);
        properties.put("enable.zookeeper", "true");
        properties.put("zookeeper.connect", zkConnString);
        properties.put("advertised.host.name", "localhost");
        return properties;
    }

    public void start() {
        System.out.println("KAFKA: Starting Embedded Kafka On Port " + kafkaServer.config().port());
        kafkaServer.startup();
        System.out.println("KAFKA: Embedded Kafka Successfully Started On Port " + kafkaServer.config().port());
    }

    public void stop(){
        System.out.println("KAFKA: Stopping Embedded Kafka");
        kafkaServer.shutdown();
        System.out.println("KAFKA: Embedded Kafka Successfully Stopped");

        System.out.println("KAFKA: Deleting Old Topics");
        deleteOldTopics();
        System.out.println("KAFKA: Successfully Deleted Old Topics");
    }

    public void deleteOldTopics() {
        //delete old Kafka topic files
        File logDir = new File(DEFAULT_LOG_DIR+"/"+TEST_TOPIC+"-0");
        if (logDir.exists()){
            deleteFolder(logDir);
        }
    }

    public static void deleteFolder(File folder) {
        File[] files = folder.listFiles();
        if(files!=null) { //some JVMs return null for empty dirs
            for(File f: files) {
                if(f.isDirectory()) {
                    System.out.println("KAFKA: Deleting " + f.getAbsolutePath());
                    deleteFolder(f);
                } else {
                    f.delete();
                }
            }
        }
        folder.delete();
    }
}