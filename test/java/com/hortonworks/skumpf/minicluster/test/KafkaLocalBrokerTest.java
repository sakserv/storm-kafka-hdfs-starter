package com.hortonworks.skumpf.minicluster.test;

import com.hortonworks.skumpf.datetime.GenerateRandomDay;
import com.hortonworks.skumpf.minicluster.KafkaLocalBroker;
import com.hortonworks.skumpf.minicluster.ZookeeperLocalCluster;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by skumpf on 12/30/14.
 */
public class KafkaLocalBrokerTest {

    private static final String TEST_TOPIC = "test_topic";

    ZookeeperLocalCluster zkCluster;
    KafkaLocalBroker kafkaLocalBroker;

    @Before
    public void setUp() {
        zkCluster = new ZookeeperLocalCluster();
        zkCluster.start();

        kafkaLocalBroker = new KafkaLocalBroker(TEST_TOPIC);
        kafkaLocalBroker.start();

    }

    @After
    public void tearDown() {

        kafkaLocalBroker.stop(true);
        zkCluster.stop();
    }

    @Test
    public void testKafkaLocalBroker() {

        kafkaLocalBroker.dumpConfig();

        //
        // Create the consumer
        //
        System.out.println("KAFKA: Configuring the consumer");
        Properties consumerProps = new Properties();
        consumerProps.put("zookeeper.connect", zkCluster.getZkConnectionString());
        consumerProps.put("group.id", "group1");
        consumerProps.put("zookeeper.session.timeout.ms", "400");
        consumerProps.put("zookeeper.sync.time.ms", "200");
        consumerProps.put("auto.commit.interval.ms", "1000");
        consumerProps.put("autooffset.reset", "smallest");

        System.out.println("KAFKA: Creating consumer connector");
        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProps));

        System.out.println("KAFKA: Getting the streams");
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(TEST_TOPIC, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(TEST_TOPIC);

        try {
            Thread.sleep(10000);
        } catch(InterruptedException e) {
            e.printStackTrace();
        }

        //
        // Add Producer properties and create the Producer
        //
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:" + kafkaLocalBroker.getPort());
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);

        // Send 10 messages to the local kafka server:
        System.out.println("KAFKA: Preparing To Send 10 Initial Messages");
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

        try {
            Thread.sleep(5000);
        } catch(InterruptedException e) {
            e.printStackTrace();
        }


        // Read the messages back
//        System.out.println("KAFKA: Reading messages from the topic");
//        for (final KafkaStream stream : streams) {
//            ConsumerIterator<byte[], byte[]> it = stream.iterator();
//            while (it.hasNext()) {
//                System.out.println("KAFKA: CONSUMER GOT: " + new String(it.next().message()));
//            }
//        }



        try {
            Thread.sleep(5000);
        } catch(InterruptedException e) {
            e.printStackTrace();
        }

    }

}
