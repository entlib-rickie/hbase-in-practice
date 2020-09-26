package com.rickie.hbase.api;

import javafx.util.Pair;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HBaseUtil {
    /**
     * 创建表
     * @param tableName 表名
     * @param familyNames 列族名
     * @return
     * @throws IOException
     */
    public static boolean createTable(String tableName, String... familyNames) throws IOException {
        Admin admin = HBaseConnectionFactory.getConnection().getAdmin();
        if(admin.tableExists(TableName.valueOf(tableName))) {
            return false;
        }

        // 通过HTableDescriptor 类描述一张表
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for(String familyName: familyNames) {
            // 通过HColumnDescriptor描述一个列族
            HColumnDescriptor oneFamily = new HColumnDescriptor(familyName);
            oneFamily.setMaxVersions(3);
            // 设置表中列族的存储生命期（1年），过期数据将自动被删除
            oneFamily.setTimeToLive(365*24*3600);
            tableDescriptor.addFamily(oneFamily);
        }
        // 构造预分区键值
        byte[][] splitKeys = new byte[9][];
        for(int i=0; i<9; i++) {
            splitKeys[i] = Bytes.toBytes(String.valueOf(i+1));
        }
        admin.createTable(tableDescriptor, splitKeys);
        admin.close();
        return true;
    }

    /**
     * 删除表
     * @param tableName 表名
     * @return
     * @throws IOException
     */
    public static boolean deleteTable(String tableName) throws IOException {
        Admin admin = HBaseConnectionFactory.getConnection().getAdmin();
        if(!admin.tableExists(TableName.valueOf(tableName))) {
            return true;
        }
        // 需要先disable表
        if(!admin.isTableDisabled(TableName.valueOf(tableName))) {
            admin.disableTable(TableName.valueOf(tableName));
        }
        admin.deleteTable(TableName.valueOf(tableName));
        admin.close();
        return true;
    }

    /**
     * 清理表中数据
     * @param tableName
     * @return
     */
    public static boolean truncateTable(String tableName) {
        try (Admin admin = HBaseConnectionFactory.getConnection().getAdmin()) {
            // 需要先disable表
            if(!admin.isTableDisabled(TableName.valueOf(tableName))) {
                admin.disableTable(TableName.valueOf(tableName));
            }
            admin.truncateTable(TableName.valueOf(tableName), false);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 创建命名空间
     * @param namespace
     * @return
     * @throws IOException
     */
    public static Boolean createNamespace(String namespace) {
        try {
            Admin admin = HBaseConnectionFactory.getConnection().getAdmin();
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(namespaceDescriptor);
            admin.close();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return  false;
    }

    /**
     * 获取
     * @param namespace
     * @return
     */
    public static String getNamespace(String namespace) {
        try{
            Admin admin = HBaseConnectionFactory.getConnection().getAdmin();
            NamespaceDescriptor nsDesc = admin.getNamespaceDescriptor(namespace);
            return nsDesc.getName();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 查询单条数据
     * @param tableName
     * @param rowkey
     * @return
     */
    public static Result getRow(String tableName, String rowkey) {
        try(Table table = HBaseConnectionFactory.getTable(tableName)) {
            Get get = new Get(Bytes.toBytes(rowkey));
            return table.get(get);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 查询多条数据
     * @param tableName
     * @param rowkeys
     * @return
     */
    public static List<Result> getRows(String tableName, List<String> rowkeys) {
        try(Table table = HBaseConnectionFactory.getTable(tableName)) {
            List<Get> gets = new ArrayList<>();
            for(String rowkey:rowkeys) {
                Get get = new Get(Bytes.toBytes(rowkey));
                gets.add(get);
            }
            Result[] results = table.get(gets);
            return Arrays.asList(results);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 根据Rowkeys获取指定的列族和列
     * @param tableName
     * @param rowkeys
     * @param family
     * @param qualifier
     * @return
     */
    public static List<Result> getCellsByRowkeys(String tableName, List<String> rowkeys, String family, String qualifier) {
        try(Table table = HBaseConnectionFactory.getTable(tableName)) {
            List<Get> gets = new ArrayList<>();
            for(String rowkey:rowkeys) {
                Get get = new Get(Bytes.toBytes(rowkey));
                if(qualifier !=null && !qualifier.isEmpty()) {
                    get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
                } else {
                    get.addFamily(Bytes.toBytes(family));
                }
                gets.add(get);
            }
            Result[] results = table.get(gets);
            return Arrays.asList(results);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 单个插入数据
     * @param tableName
     * @param rowKey
     * @param cfName
     * @param qualifier
     * @param data
     * @return
     */
    public static boolean putCell(String tableName, String rowKey, String cfName, String qualifier, String data) {
        try(Table table = HBaseConnectionFactory.getTable(tableName)) {
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(qualifier), Bytes.toBytes(data));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * 批量插入数据
     * @param tableName
     * @param puts
     * @return
     */
    public static boolean putRows(String tableName, List<Put> puts) {
        try(Table table = HBaseConnectionFactory.getTable(tableName)) {
            table.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    /**
     * 批量插入数据
     * @param tableName
     * @param rowkey
     * @param cfName
     * @param pairList
     * @return
     */
    public static boolean putRows(String tableName, String rowkey, String cfName,
                                  List<Pair<String, String>> pairList) {
        try(Table table = HBaseConnectionFactory.getTable(tableName)) {
            Put put = new Put(rowkey.getBytes());
            pairList.forEach(pair ->
                    put.addColumn(cfName.getBytes(), pair.getKey().getBytes(), pair.getValue().getBytes()));
            table.put(put);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 批量插入数据（BufferedMutator实现）
     * @param tableName
     * @param puts
     * @return
     */
    public static boolean putRowsBuffered(String tableName, List<Put> puts) {
        try {
            // 设置cache=1M，当达到1M时数据会自动刷到HBase
            BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableName)).writeBufferSize(1024*1024);
            BufferedMutator mutator = HBaseConnectionFactory.getConnection().getBufferedMutator(params);
            // 向HBase插入数据,达到缓存会自动提交
            mutator.mutate(puts);
            // mutator.flush();
            mutator.close();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    /**
     * scan扫描数据
     * @param tableName
     * @return
     */
    public static ResultScanner getScanner(String tableName) {
        try(Table table = HBaseConnectionFactory.getTable(tableName)) {
            Scan scan = new Scan();
            // 每次RPC请求的记录数
            scan.setCaching(50);
            ResultScanner results = table.getScanner(scan);
            return results;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 通过指定前缀获取Rowkey列表
     * @param tableName
     * @param prefix
     * @return
     */
    public static List<String> getRowkeysByPrefix(String tableName, String prefix) {
        ArrayList<String> list = new ArrayList<>();
        try(Table table = HBaseConnectionFactory.getTable(tableName)) {
            Scan scan = new Scan();
            scan.setRowPrefixFilter(Bytes.toBytes(prefix));
            ResultScanner scanner = table.getScanner(scan);
            for(Result result:scanner) {
                list.add(Bytes.toString(result.getRow()));
            }
            scanner.close();
            return list;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 获取startRow和stopRow之间的Rowkey列表
     * @param tableName
     * @param startRow
     * @param stopRow
     * @return
     */
    public static List<String> getRowkeysByRange(String tableName, String startRow, String stopRow) {
        List<String> list = new ArrayList<>();
        try(Table table = HBaseConnectionFactory.getTable(tableName)) {
            Scan scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(stopRow));
            ResultScanner scanner = table.getScanner(scan);
            for(Result result:scanner) {
                list.add(Bytes.toString(result.getRow()));
            }
            scanner.close();
            return list;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 根据条件扫描表
     * @param tableName
     * @param filterList
     * @return
     */
    public static ResultScanner filterTable(String tableName, FilterList filterList) {
        try(Table table = HBaseConnectionFactory.getTable(tableName)) {
            Scan scan = new Scan();
            scan.setFilter(filterList);
            return table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 删除行
     * @param tableName
     * @param rowKey
     * @return
     */
    public static boolean deleteRow(String tableName, String rowKey) {
        try(Table table = HBaseConnectionFactory.getTable(tableName)) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    /**
     * 删除列
     * @param tableName
     * @param rowKey
     * @param cfName
     * @param qualifierName
     * @return
     */
    public static boolean deleteQualifier(String tableName, String rowKey, String cfName, String qualifierName) {
        try(Table table = HBaseConnectionFactory.getTable(tableName)) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            delete.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(qualifierName));
            table.delete(delete);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }
}
