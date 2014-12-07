package com.hortonworks.skumpf.storm.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import java.io.IOException;

/**
 * Created by skumpf on 12/6/14.
 */
public class HdfsCluster {

    private static MiniDFSCluster.Builder clusterBuilder;
    private static MiniDFSCluster cluster;
    private static final String DEFAULT_LOG_DIR = "/tmp/embedded/hdfs/";

    public HdfsCluster() {
        clusterBuilder = new MiniDFSCluster.Builder(createConfiguration());
    }

    private static Configuration createConfiguration() {
        Configuration conf = new Configuration();
        conf.setBoolean("dfs.permissions", false);
        return conf;
    }

    public void startHdfs() {
        System.out.println("HDFS: Starting MiniDfsCluster");
        try {

            cluster = clusterBuilder.numDataNodes(1)
            .format(true)
            .racks(null)
            .build();

            System.out.println("HDFS: MiniDfsCluster started at " + cluster.getFileSystem().getCanonicalServiceName());
        } catch(IOException e) {
            System.out.println("ERROR: Failed to start MiniDfsCluster");
            System.exit(3);
        }
    }

    public void stopHdfs() {
        System.out.println("HDFS: Stopping MiniDfsCluster");
        cluster.shutdown();
        System.out.println("HDFS: MiniDfsCluster Stopped");

    }

    public String getHdfsUriString() {
        try {
            return "hdfs://" + cluster.getFileSystem().getCanonicalServiceName();
        } catch(IOException e) {
            System.out.println("ERROR: Failed to return MiniDFsCluster URI");
            System.exit(3);
        }
        return "";
    }

    public FileSystem getHdfsFileSystemHandle() {
        try {
            return cluster.getFileSystem();
        } catch(IOException e) {
            System.out.println("ERROR: Failed to return MiniDFsCluster URI");
            System.exit(3);
        }
        return null;
    }
}
