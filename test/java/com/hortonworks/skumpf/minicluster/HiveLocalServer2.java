package com.hortonworks.skumpf.minicluster;

import com.hortonworks.skumpf.util.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.Service;
import org.apache.hive.service.server.HiveServer2;

/**
 * Created by skumpf on 12/20/14.
 */
public class HiveLocalServer2 implements MiniCluster {

    private static final String DEFAULT_METASTORE_URI = "";
    private static final String DEFAULT_DERBY_DB_PATH = "metastore_db";
    private static final String DEFAULT_HIVE_SCRATCH_DIR = "hive_scratch_dir";
    private static final int DEFAULT_HIVESERVER2_PORT = 10000;

    private HiveConf hiveConf = new HiveConf();
    private HiveServer2 server;

    private String metaStoreUri;
    private String derbyDbPath;
    private String hiveScratchDir;
    private int hiveServer2Port;

    private String zookeeperQuorum;

    public HiveLocalServer2() {
        metaStoreUri = DEFAULT_METASTORE_URI;
        derbyDbPath = DEFAULT_DERBY_DB_PATH;
        hiveScratchDir = DEFAULT_HIVE_SCRATCH_DIR;
        hiveServer2Port = DEFAULT_HIVESERVER2_PORT;
        configure();
    }

    public HiveLocalServer2(String metaStoreUri, String derbyDbPath, String hiveScratchDir, int hiveServer2Port) {
        this.metaStoreUri = metaStoreUri;
        this.derbyDbPath = derbyDbPath;
        this.hiveScratchDir = hiveScratchDir;
        this.hiveServer2Port = hiveServer2Port;
        configure();
    }

    public HiveLocalServer2(String metaStoreUri, String derbyDbPath, String hiveScratchDir,
                            int hiveServer2Port, String zookeeperQuorum) {
        this.metaStoreUri = metaStoreUri;
        this.derbyDbPath = derbyDbPath;
        this.hiveScratchDir = hiveScratchDir;
        this.hiveServer2Port = hiveServer2Port;
        this.zookeeperQuorum = zookeeperQuorum;
        configure();
        configureWithZookeeper();
    }

    public void configure() {
        hiveConf.set("hive.root.logger", "DEBUG,console");
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, metaStoreUri);
        hiveConf.set(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname, "jdbc:derby:;databaseName=" +
                derbyDbPath + ";create=true");
        hiveConf.set(HiveConf.ConfVars.SCRATCHDIR.varname, hiveScratchDir);
        hiveConf.set(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT.varname, String.valueOf(hiveServer2Port));
    }

    public void configureWithZookeeper() {
        hiveConf.set(HiveConf.ConfVars.HIVE_ZOOKEEPER_QUORUM.varname, zookeeperQuorum);
    }

    public void start() {
        server = new HiveServer2();
        System.out.println("HIVESERVER2: Starting HiveServer2 on port: " + hiveServer2Port);
        server.init(hiveConf);
        server.start();
    }

    public void stop() {
        System.out.println("HIVESERVER2: Stopping HiveServer2 on port: " + hiveServer2Port);
        server.stop();
    }

    public void stop(boolean cleanUp) {
        stop();
        if (cleanUp) {
            cleanUp();
        }
    }

    private void cleanUp() {
        FileUtils.deleteFolder(derbyDbPath);
        FileUtils.deleteFolder(hiveScratchDir);
    }

    public String getHiveServerThriftPort() {
        return server.getHiveConf().get(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT.varname);
    }

    public void dumpConfig() {
        for(Service service: server.getServices()) {
            System.out.println("HIVE: HiveServer2 Services Name:" + service.getName() +
                    " CONF: " + String.valueOf(service.getHiveConf().getAllProperties()));
        }
    }

}
