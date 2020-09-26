package com.rickie.hbase.api;

import com.rickie.hbase.RowkeyUtil;
import javafx.util.Pair;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnValueFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

class HBaseUtilTest {
    @Test
    void createNamespaceTest() {
        HBaseUtil.createNamespace("orders");
        System.out.println(HBaseUtil.getNamespace("orders"));
    }

    @Test
    void createTableTest() throws IOException {
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat format = new SimpleDateFormat("yyyyMM");
        Boolean result = HBaseUtil.createTable("orders" + format.format(calendar.getTime()), "info", "items");
        Assert.assertTrue(result);
    }

    @Test
    void deleteTableTest() throws IOException {
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat format = new SimpleDateFormat("yyyyMM");
        calendar.add(Calendar.MONTH, -1);
        Boolean result = HBaseUtil.deleteTable("orders"+format.format(calendar.getTime()));
        Assert.assertTrue(result);
    }

    @Test
    public void truncateTableTest() {
        Boolean result = HBaseUtil.truncateTable(TABLE);
        Assert.assertTrue(result);
    }

    private static final String TABLE="orders202008";
    private static final String CF_INFO="info";
    private static final String CF_ITEMS="items";
    private static final String[] ITEM_ID_ARRAY = new String[]{"1001","1002","1003","1004"};
    private static final String[] ITEM_NAME_ARRAY = new String[]{"Huawei Mate20","Thinkpad","Xiaomi","Vivo"};

    @Test
    void putRowsTest() {
        List<Put> puts = new ArrayList<Put>();
        Random random = new Random();
        for (int i = 0; i < 5; i++) {
            long orderId = random.nextInt(10000);
            String userId = "user-" + orderId;
            String rowkey = RowkeyUtil.generateRowkey(orderId);
            Put put = new Put(Bytes.toBytes(rowkey));
            //添加列
            put.addColumn(Bytes.toBytes(CF_INFO), Bytes.toBytes("order_id"), Bytes.toBytes(String.valueOf(orderId)));
            put.addColumn(Bytes.toBytes(CF_INFO), Bytes.toBytes("user_id"), Bytes.toBytes(userId));
            put.addColumn(Bytes.toBytes(CF_INFO), Bytes.toBytes("amount"), Bytes.toBytes("1000"));
            put.addColumn(Bytes.toBytes(CF_INFO), Bytes.toBytes("mobile"), Bytes.toBytes("13012345678"));
            put.addColumn(Bytes.toBytes(CF_INFO), Bytes.toBytes("address"), Bytes.toBytes("Shanghai, China"));

            if(random.nextBoolean()){
                for (int j = 0; j < ITEM_ID_ARRAY.length; j++) {
                    put.addColumn(Bytes.toBytes(CF_ITEMS), Bytes.toBytes(ITEM_ID_ARRAY[j]),
                            Bytes.toBytes("item_name='" + ITEM_NAME_ARRAY[j] + "';price=2000;quantity=1"));
                }
            }
            puts.add(put);
        }

        HBaseUtil.putRows(TABLE, puts);
    }

    @Test
    public void putRowsPairTest() {
        List<Pair<String, String>> pairList, pairItemList = null;
        Random random = new Random();
        for (int i = 0; i < 5; i++) {
            long orderId = random.nextInt(10000);
            String userId = "user-" + orderId;
            String rowkey = RowkeyUtil.generateRowkey(orderId);

            // 添加列
            pairList = Arrays.asList(
                    new Pair<>("order_id", String.valueOf(orderId)),
                    new Pair<>("user_id", userId),
                    new Pair<>("amount", "1100"),
                    new Pair<>("mobile", "18812345678"),
                    new Pair<>("address", "Beijing, China"));
            HBaseUtil.putRows(TABLE, rowkey, CF_INFO, pairList);

            if(random.nextBoolean()){
                for (int j = 0; j < ITEM_ID_ARRAY.length; j++) {
                    pairItemList = Arrays.asList(
                      new Pair<>(ITEM_ID_ARRAY[j],
                              "item_name='" + ITEM_NAME_ARRAY[j] + "';price=2000;quantity=1")
                    );
                }
                if(pairItemList != null && !pairItemList.isEmpty()) {
                    HBaseUtil.putRows(TABLE, rowkey, CF_ITEMS, pairItemList);
                }
            }
        }
    }

    @Test
    void putRowsBufferedTest() {
        List<Put> puts = new ArrayList<Put>();
        Random random = new Random();
        for (int i = 0; i < 5; i++) {
            long orderId = random.nextInt(10000);
            String userId = "user-" + orderId;
            String rowkey = RowkeyUtil.generateRowkey(orderId);
            Put put = new Put(Bytes.toBytes(rowkey));
            //添加列
            put.addColumn(Bytes.toBytes(CF_INFO), Bytes.toBytes("order_id"), Bytes.toBytes(String.valueOf(orderId)));
            put.addColumn(Bytes.toBytes(CF_INFO), Bytes.toBytes("user_id"), Bytes.toBytes(userId));
            put.addColumn(Bytes.toBytes(CF_INFO), Bytes.toBytes("amount"), Bytes.toBytes("1000"));
            put.addColumn(Bytes.toBytes(CF_INFO), Bytes.toBytes("mobile"), Bytes.toBytes("13012345678"));
            put.addColumn(Bytes.toBytes(CF_INFO), Bytes.toBytes("address"), Bytes.toBytes("Shanghai, China"));

            if(random.nextBoolean()){
                for (int j = 0; j < ITEM_ID_ARRAY.length; j++) {
                    put.addColumn(Bytes.toBytes(CF_ITEMS), Bytes.toBytes(ITEM_ID_ARRAY[j]),
                            Bytes.toBytes("item_name='" + ITEM_NAME_ARRAY[j] + "';price=2000;quantity=1"));
                }
            }
            puts.add(put);
        }

        HBaseUtil.putRowsBuffered(TABLE, puts);
    }

    private long orderId = 123456l;
    private String rowkey = RowkeyUtil.generateRowkey(orderId);
    @Test
    void putRowTest() {
//        Random random = new Random();
//        long orderId = random.nextInt(10000);
//        String rowkey = RowKeyUtil.generateRowkey(orderId);
        HBaseUtil.putCell(TABLE, rowkey, CF_INFO, "order_id", String.valueOf(orderId));
    }

    @Test
    public void getRowTest() {
        Result result = HBaseUtil.getRow(TABLE, rowkey);
        System.out.println("查询单行");
        if(result != null && result.rawCells().length >0) {
            Cell[] cells = result.rawCells();
            for(Cell cell : cells) {
                String row = Bytes.toString(CellUtil.cloneRow(cell));
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println("row: " + row + "\t" + family + ":" +column + "\t" + value);
            }
        } else {
            System.out.println("空结果 ... ");
        }
    }

    @Test
    public void getRowsTest() {
        // 替换不同的rowkey
        List<String> rowkeys = Arrays.asList("33260000000000000000", "44480000000000000000");
        List<Result> results = HBaseUtil.getRows(TABLE, rowkeys);
        if(results != null) {
            results.forEach(result -> System.out.println(Bytes.toString(result.getRow())));
        }
    }

    @Test
    public void getCellsByRowkeysTest() {
        List<String> rowkeys = Arrays.asList("33260000000000000000", "44480000000000000000");
        List<Result> results = HBaseUtil.getCellsByRowkeys(TABLE, rowkeys, CF_INFO, "order_id");
        if(results != null) {
            results.forEach(result -> {
                System.out.println(Bytes.toString(result.getRow()));
                System.out.println("info:order_id = " + Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("order_id"))));
            });
        }
    }

    @Test
    public void getScannerTest() {
        ResultScanner results = HBaseUtil.getScanner(TABLE);
        if(results != null) {
//            results.forEach(result -> {
//                System.out.println("rowKey = " + Bytes.toString(result.getRow()));
//                System.out.println("info:order_id = " + Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("order_id"))));
//                System.out.println("info:user_id = " + Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("user_id"))));
//                System.out.println("info:amount = " + Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("amount"))));
//            });

            for(Result result:results) {
                // 单元格集合
                List<Cell> cells = result.listCells();
                for(Cell cell:cells) {
                    String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
                    long timestamp = cell.getTimestamp();
                    String family = Bytes.toString(CellUtil.cloneFamily(cell));
                    String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    System.out.println("==> rowkey: " + rowkey + ", timestamp: " +
                            timestamp + ", " + family +":" + qualifier +"=" + value);
                }
            }

            results.iterator().forEachRemaining(result-> System.out.println(new String(result.value())));
        }
    }

    @Test
    public void getRowkeysByPrefixTest() {
        List<String> rowkeys = HBaseUtil.getRowkeysByPrefix(TABLE, "6");
        System.out.println(rowkeys);
    }

    @Test
    public void getRowkeysByRangeTest() {
        List<String> rowkeys = HBaseUtil.getRowkeysByRange(TABLE, "1", "70000000000000000000");
        System.out.println(rowkeys);
    }

    @Test
    public void filterTableTest() {
        // 通过添加 filters 来进行条件过滤
        ColumnValueFilter columnValueFilter = new ColumnValueFilter(CF_INFO.getBytes(), "order_id".getBytes(), CompareOperator.GREATER_OR_EQUAL, "5000".getBytes());
        FilterList filterList = new FilterList(columnValueFilter);
        ResultScanner scanner = HBaseUtil.filterTable(TABLE, filterList);
        scanner.iterator().forEachRemaining(result -> System.out.println(new String(result.value())));
    }

    @Test
    public void deleteRowTest(){
        HBaseUtil.deleteRow(TABLE, rowkey);
    }
}