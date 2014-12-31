package com.hortonworks.skumpf.minicluster;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.Service;
import org.apache.hive.service.server.HiveServer2;

/**
 * Created by skumpf on 12/20/14.
 */
public class HiveLocalServer2 implements MiniCluster {

    HiveConf hiveConf = new HiveConf();
    HiveServer2 server;

    public HiveLocalServer2() {
        configure();
    }

    public HiveLocalServer2(String metaStoreUri) {
        configure();
        hiveConf.set("hive.metastore.uris", metaStoreUri);
    }

    public void configure() {
        hiveConf.set(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname, "jdbc:derby:;databaseName=metastore_db;create=true");
        hiveConf.set("hive.root.logger", "DEBUG,console");
    }

    public void start() {
        server = new HiveServer2();
        System.out.println("HIVE: Starting HiveLocalServer.");
        server.init(hiveConf);
        server.start();
        System.out.println("HIVE: HiveLocalServer successfully started.");
    }

    public void stop() {
        System.out.println("HIVE: Stopping HiveLocalServer.");
        server.stop();
        System.out.println("HIVE: HiveLocalServer successfully stopped.");
    }

    public String getHiveServerThriftPort() {
        return server.getHiveConf().get("hive.server2.thrift.port");
    }

    public void dumpConfig() {
        for(Service service: server.getServices()) {
            System.out.println("HIVE: HiveServer2 Services Name:" + service.getName() + " CONF: " + String.valueOf(service.getHiveConf().getAllProperties()));
        }
    }

}
