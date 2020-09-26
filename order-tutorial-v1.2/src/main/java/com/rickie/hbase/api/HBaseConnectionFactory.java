package com.rickie.hbase.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

public class HBaseConnectionFactory {
    private static Connection connection = null;

    static{
        createConnection();
        System.out.println("========= create connection ============");
    }

    private static synchronized void createConnection() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.client.pause", "100");
        configuration.set("hbase.client.write.buffer", "10485760");
        configuration.set("hbase.client.retries.number", "5");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.client.scanner.timeout.period", "100000");
        configuration.set("hbase.rpc.timeout", "40000");
        configuration.set("hbase.zookeeper.quorum", "10.131.236.114,10.131.236.115,10.131.236.116");

        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static Connection getConnection() {
        return connection;
    }

    /**
     * 获取HBase Table 对象
     * @param tableName
     * @return
     * @throws IOException
     */
    public static Table getTable(String tableName) throws IOException {
        return getConnection().getTable(TableName.valueOf(tableName));
    }
}
