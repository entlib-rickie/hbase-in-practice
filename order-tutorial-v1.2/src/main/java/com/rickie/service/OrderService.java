package com.rickie.service;

import com.rickie.hbase.RowkeyUtil;
import com.rickie.hbase.api.HBaseUtil;
import com.rickie.model.Order;
import com.rickie.model.OrderItem;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class OrderService {
    private static final String TABLE="orders:orders202009";
    private static final String CF_INFO="info";
    private static final String CF_ITEMS="items";

    public Order getOrderByOrderId(long orderId){
        Result result = HBaseUtil.getRow(TABLE, RowkeyUtil.generateRowkey(orderId));
        if(result != null && result.rawCells().length >0) {
            Order order = new Order();
            order.setOrderId(orderId);
            order.setUserId(Bytes.toString(result.getValue(CF_INFO.getBytes(), "user_id".getBytes())));
            order.setAmount(Double.parseDouble(Bytes.toString(result.getValue(CF_INFO.getBytes(), "amount".getBytes()))));
            order.setMobile(Bytes.toString(result.getValue(CF_INFO.getBytes(), "mobile".getBytes())));
            order.setAddress(Bytes.toString(result.getValue(CF_INFO.getBytes(), "address".getBytes())));
            // 获取HBase指定列族中的所有列
            Map<byte[], byte[]> itemMap = result.getFamilyMap(CF_ITEMS.getBytes());
            if(itemMap != null) {
                for(Map.Entry<byte[], byte[]> entry:itemMap.entrySet()){
                    // key为列族中的列名，同时也为item id
                    String columnName = Bytes.toString(entry.getKey());
                    // 示例记录：column=items:1002, timestamp=1600424705487,
                    // value=item_name='Thinkpad';price=2000;quantity=1
                    String itemValue = Bytes.toString(entry.getValue());
                    String[] itemInfo = itemValue.split(";");
                    OrderItem orderItem = new OrderItem();
                    // 设置item的属性值
                    orderItem.setItemId(Integer.parseInt(columnName));
                    orderItem.setItemName(itemInfo[0].split("=")[1]);
                    orderItem.setPrice(Double.parseDouble(itemInfo[1].split("=")[1]));
                    orderItem.setQuantity(Integer.parseInt(itemInfo[2].split("=")[1]));
                    order.addOrderItem(orderItem);
                }
            }
            return order;
        } else {
            System.out.println("空结果 ... ");
        }
        return null;
    }
}
