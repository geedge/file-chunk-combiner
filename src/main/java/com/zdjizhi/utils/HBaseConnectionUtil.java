package com.zdjizhi.utils;

import com.zdjizhi.config.Configs;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionConfiguration;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class HBaseConnectionUtil {

    private static HBaseConnectionUtil hbaseConnectionUtil = null;
    private final org.apache.hadoop.conf.Configuration hbaseConfiguration;

    private HBaseConnectionUtil(Configuration configuration) {
        hbaseConfiguration = HBaseConfiguration.create();
        hbaseConfiguration.set(HConstants.ZOOKEEPER_QUORUM, configuration.getString(Configs.SINK_HBASE_ZOOKEEPER));
        hbaseConfiguration.set(HConstants.CLIENT_ZOOKEEPER_CLIENT_PORT, "2181");
        hbaseConfiguration.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase");
        hbaseConfiguration.set(HConstants.BULKLOAD_MAX_RETRIES_NUMBER, configuration.get(Configs.SINK_HBASE_RETRIES_NUMBER) + "");
        hbaseConfiguration.set(HConstants.HBASE_RPC_TIMEOUT_KEY, configuration.get(Configs.SINK_HBASE_RPC_TIMEOUT) + "");
        hbaseConfiguration.set(ConnectionConfiguration.MAX_KEYVALUE_SIZE_KEY, "1073741800");
        hbaseConfiguration.set(ConnectionConfiguration.WRITE_BUFFER_SIZE_KEY, configuration.get(Configs.SINK_HBASE_CLIENT_WRITE_BUFFER) + "");
        hbaseConfiguration.set(HConstants.HBASE_CLIENT_IPC_POOL_SIZE, configuration.get(Configs.SINK_HBASE_CLIENT_IPC_POOL_SIZE) + "");

    }

    public static synchronized HBaseConnectionUtil getInstance(Configuration configuration) {
        if (null == hbaseConnectionUtil) {
            hbaseConnectionUtil = new HBaseConnectionUtil(configuration);
        }
        return hbaseConnectionUtil;
    }

    public Connection getSyncHBaseConnection() {
        Connection syncHBaseConnection;
        try {
            syncHBaseConnection = ConnectionFactory.createConnection(hbaseConfiguration);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
        return syncHBaseConnection;
    }

    public AsyncConnection getAsyncHBaseConnection() {
        AsyncConnection asyncHBaseConnection;
        try {
            asyncHBaseConnection = ConnectionFactory.createAsyncConnection(hbaseConfiguration).get();
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e.getMessage());
        }
        return asyncHBaseConnection;
    }
}
