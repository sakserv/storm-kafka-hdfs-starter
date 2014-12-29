package com.hortonworks.skumpf.storm;
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.hortonworks.skumpf.storm.bolt.PrinterBolt;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hive.bolt.HiveBolt;
import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper;
import org.apache.storm.hive.bolt.mapper.JsonRecordHiveMapper;
import org.apache.storm.hive.common.HiveOptions;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.util.UUID;


public class KafkaHiveTopology {


    public static void configureKafkaSpout(TopologyBuilder builder, String zkHostString, String kafkaTopic, String kafkaStartOffset) {

        // Configure the KafkaSpout
        SpoutConfig spoutConfig = new SpoutConfig(new ZkHosts(zkHostString),
                kafkaTopic,      // Kafka topic to read from
                "/" + kafkaTopic, // Root path in Zookeeper for the spout to store consumer offsets
                UUID.randomUUID().toString());  // ID for storing consumer offsets in Zookeeper
        //spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.scheme = new SchemeAsMultiScheme(new TestScheme());

        // Allow for passing in an offset time
        // startOffsetTime has a bug that ignores the special -2 value
        if(kafkaStartOffset == "-2") {
            spoutConfig.forceFromStart = true;
        } else if (kafkaStartOffset != null) {
            spoutConfig.startOffsetTime = Long.parseLong(kafkaStartOffset);
        }
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        // Add the spout and bolt to the topology
        builder.setSpout("kafkaspout", kafkaSpout, 1);

    }

    public static void configureHiveStreamingBolt(TopologyBuilder builder, String[] colNames, String[] partitionCol, String metastoreUri, String dbName, String tableName) {

        DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper()
                .withColumnFields(new Fields(colNames));
                //.withPartitionFields(new Fields(partitionCol));
        HiveOptions hiveOptions = new HiveOptions(metastoreUri, dbName, tableName, mapper)
                .withAutoCreatePartitions(true)
                .withTxnsPerBatch(100)
                .withMaxOpenConnections(100)
                .withBatchSize(1000)
                .withIdleTimeout(3600)
                .withHeartBeatInterval(240);
        HiveBolt bolt = new HiveBolt(hiveOptions);
        builder.setBolt("hivebolt", bolt, 1).shuffleGrouping("kafkaspout");

    }

    public static void main(String[] args) throws Exception {

        if (args.length < 7) {
            System.out.println("USAGE: storm jar </path/to/topo.jar> <com.package.TopologyMainClass> " +
                    "<topo_display_name> <zookeeper_host:port[,zookeeper_host:port]> " +
                    "<kafka_topic_name> <offset_time_to_start_from> <hivecol1,[hivecol2]> " +
                    "<hivepartition1,[hivepartiton2]> <metastoreUri> <hivedb> <hivetable>");
            System.exit(3);
        }

        TopologyBuilder builder = new TopologyBuilder();

        // Setup the Kafka Spout
        configureKafkaSpout(builder, args[1], args[2], args[3]);

        // Setup the Hive Bolt
        String[] cols = args[4].split(",");
        String[] parts = {args[5]};
        configureHiveStreamingBolt(builder, cols, parts, args[6], args[7], args[8]);

        // Topology
        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(1);

        // Submit the topology
        StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());

    }
}
