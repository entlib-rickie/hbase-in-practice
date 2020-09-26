package com.rickie.hbase;

public class RowkeyUtil {
    public static String formatOrderId(long orderId) {
        // 数字前面补0
        String str = String.format("%0" + 20 + "d", orderId);
        StringBuilder sb = new StringBuilder(str);
        return sb.reverse().toString();
    }

    public static String generateRowkey(long orderId){
        return formatOrderId(orderId);
    }
}
