package com.hortonworks.skumpf.storm;

import com.hortonworks.skumpf.storm.tools.HdfsCluster;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;


/**
 * Created by skumpf on 12/6/14.
 */
public class HdfsClusterTest {

    private HdfsCluster dfsCluster;

    @Before
    public void setUp(){
        dfsCluster = new HdfsCluster();
        dfsCluster.startHdfs();
    }

    @After
    public void tearDown(){
        dfsCluster.stopHdfs();
    }

    @Test
    public void testDfsClusterStart() {
        System.out.println("HDFS: Cluster URI: " + dfsCluster.getHdfsUriString());

        FileSystem hdfsFsHandle = dfsCluster.getHdfsFileSystemHandle();
        try {
            FSDataOutputStream writer = hdfsFsHandle.create(new Path("/tmp/testing"));
            writer.writeUTF("This is a test");
            writer.close();

            FSDataInputStream reader = hdfsFsHandle.open(new Path("/tmp/testing"));
            System.out.println("HDFS READ: Output from test file: " + reader.readUTF());
            reader.close();

            hdfsFsHandle.close();
        } catch(IOException e) {
            System.out.println(e);
        }

    }
}
