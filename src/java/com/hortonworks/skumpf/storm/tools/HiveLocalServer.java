package com.hortonworks.skumpf.storm.tools;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hive.service.Service;
import org.apache.hive.service.server.HiveServer2;

/**
 * Created by skumpf on 12/20/14.
 */
public class HiveLocalServer {

    HiveConf hiveConf;
    HiveServer2 server = new HiveServer2();

    public HiveLocalServer() {
        hiveConf = new HiveConf(getClass());
        hiveConf.set("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=/tmp/metastore_db;create=true");
        hiveConf.set("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver");
        hiveConf.set("hive.metastore.warehouse.dir", "/tmp/warehouse_dir");
        hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
        hiveConf.set("hive.root.logger", "DEBUG,console");
    }

    public void start() {
        System.out.println("HIVE: Starting HiveLocalServer.");
        server.init(hiveConf);
        System.out.println("HIVE: HS2 Name: " + server.getName());
        server.start();
        System.out.println("HIVE: HiveLocalServer successfully started.");
    }

    public void stop() {
        System.out.println("HIVE: Stopping HiveLocalServer.");
        server.stop();
        System.out.println("HIVE: HiveLocalServer successfully stopped.");
    }

    public String getMetastoreUri() {
        String metastoreUri = String.valueOf(server.getHiveConf().get("hive.metastore.uris"));
        System.out.println("HIVE: Metastore URI is: " + metastoreUri);
        return metastoreUri;
    }

    public String getHiveServerThriftPort() {
        return server.getHiveConf().get("hive.server2.thrift.port");
    }

    public void dumpConfig() {
        for(Service service: server.getServices()) {
            System.out.println("HIVE: HiveServer2 Services Name:" + service.getName() + " CONF: " + String.valueOf(service.getHiveConf().getAllProperties()));
        }
        //System.out.println("HIVE: HiveServer2 Config: " + String.valueOf(server.getHiveConf().getAllProperties()));
    }

}
