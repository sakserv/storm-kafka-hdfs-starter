package com.hortonworks.skumpf.minicluster;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.Service;
import org.apache.hive.service.server.HiveServer2;

/**
 * Created by skumpf on 12/20/14.
 */
public class HiveLocalServer2 {

    HiveConf hiveConf;
    HiveServer2 server = new HiveServer2();

    public HiveLocalServer2() {
        configureHiveServer();
    }

    public HiveLocalServer2(String metaStoreUri) {
        configureHiveServer();
        hiveConf.set("hive.metastore.uris", metaStoreUri);
    }

    public void configureHiveServer() {
        hiveConf = new HiveConf(getClass());
        hiveConf.set("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=metastore_db");
        //hiveConf.set("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver");
        //hiveConf.set("hive.metastore.warehouse.dir", "/tmp/warehouse_dir");
        //hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
        //hiveConf.set("hive.root.logger", "DEBUG,console");
        //hiveConf.set("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
        //hiveConf.set("hive.compactor.initiator.on", "true");
        //hiveConf.set("hive.compactor.worker.threads", "5");
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

    public String getHiveServerThriftPort() {
        return server.getHiveConf().get("hive.server2.thrift.port");
    }

    public void dumpConfig() {
        for(Service service: server.getServices()) {
            System.out.println("HIVE: HiveServer2 Services Name:" + service.getName() + " CONF: " + String.valueOf(service.getHiveConf().getAllProperties()));
        }
    }

}
