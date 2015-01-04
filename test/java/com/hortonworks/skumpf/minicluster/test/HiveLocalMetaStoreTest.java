package com.hortonworks.skumpf.minicluster.test;

import com.hortonworks.skumpf.minicluster.HiveLocalMetaStore;
import com.hortonworks.skumpf.minicluster.HiveLocalServer2;
import com.hortonworks.skumpf.util.FileUtils;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by skumpf on 12/30/14.
 */
public class HiveLocalMetaStoreTest {

    private static final String HIVE_DB_NAME = "default";
    private static final String HIVE_TABLE_NAME = "test_table";
    private static final String HIVE_TABLE_PATH = new File(HIVE_TABLE_NAME).getAbsolutePath();
    HiveLocalMetaStore hiveServer;

    @Before
    public void setUp() {
        hiveServer = new HiveLocalMetaStore();
        hiveServer.start();
    }

    @After
    public void tearDown() {
        hiveServer.stop(true);
        FileUtils.deleteFolder(HIVE_TABLE_PATH);
    }

    @Test
    public void testHiveLocalMetaStore() {

        // Create a table and display it back
        try {
            HiveMetaStoreClient hiveClient = new HiveMetaStoreClient(hiveServer.getConf());

            hiveClient.dropTable(HIVE_DB_NAME, HIVE_TABLE_NAME, true, true);

            // Define the cols
            List<FieldSchema> cols = new ArrayList<FieldSchema>();
            cols.add(new FieldSchema("id", Constants.INT_TYPE_NAME, ""));
            cols.add(new FieldSchema("msg", Constants.STRING_TYPE_NAME, ""));

            // Values for the StorageDescriptor
            String location = HIVE_TABLE_PATH;
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

    }

}
