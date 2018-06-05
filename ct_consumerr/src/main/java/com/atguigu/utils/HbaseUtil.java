package com.atguigu.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;

public class HbaseUtil {

    private static Configuration conf = HBaseConfiguration.create();
    public static  boolean isTableExist(String tableName) throws IOException {
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        boolean result = admin.tableExists(TableName.valueOf(tableName));
        close(conn,admin);
        return result;


        }


    //初始化命名空间
    public static void initNamespace(String namespace) throws IOException {
        //获取连接
        Connection conn = ConnectionFactory.createConnection(conf);
        //获取admin对象
        Admin admin = conn.getAdmin();
        //创建namespace描述器
        NamespaceDescriptor descriptor = NamespaceDescriptor.create(namespace).build();
        admin.createNamespace(descriptor);
        close(conn,admin);
    }
    // 创建表
    public static void createTable(String tableName,int regions,String...columnFamily) throws IOException {
      if (isTableExist(tableName)){
          System.out.println("表"+tableName+"已经存在!!");
          return;
      }
       //获取连接
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        //创建表描述其
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        //添加列祖
        for (String cf: columnFamily) {
            hTableDescriptor.addFamily(new HColumnDescriptor(cf));
        }
        //添加协调处理器
        hTableDescriptor.addCoprocessor("com.atguigu.coprocessor.CalleeWriteObserver");
        //创建表
        admin.createTable(hTableDescriptor,getSplitkeys(regions));
          close(conn,admin);
    }
//预分区键
    public static byte[][] getSplitkeys(int regions) {
        DecimalFormat df = new DecimalFormat("00");
        byte[][] splitKeys = new byte[regions][];
        for (int i = 0; i < regions; i++) {
            splitKeys[i] = Bytes.toBytes(df.format(i) + "|");
        }
            for (byte[] splitKey : splitKeys) {
                System.out.println(Bytes.toString(splitKey));
            }
            return splitKeys;
        }
        /*
           生成rowkey,xxx13651234567_2019-02-21 13:13:13_13891234567_0180
         * regionHash_caller_buildTime_callee_duration
       */
        public static String getRowKey(String regionHash,String caller,String buildTime,
                                          String callee,String flag,String duration){

           return regionHash+"_"+caller+"_"+buildTime+"_"+callee+"_"+duration;
        }

     /*
     生成分区号

      */
    public static String getRegionHash(String caller,String buildTime,int regions){
         int len=caller.length();
        String last4Num=caller.substring(len-4);
    //获取年月
        String yearMonth = buildTime.replaceAll("-", "").substring(0, 6);
        int regionCode = (Integer.valueOf(last4Num) ^ Integer.valueOf(yearMonth)) % regions;
        DecimalFormat df = new DecimalFormat("00");
        return df.format(regionCode);


    }












    //关闭资源
    private static void close(Connection conn, Admin admin, Table...tables){
        if (conn!=null){
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (admin!=null){
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (tables.length<=0) return ;
        for (Table table : tables) {
            if(table!=null){
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }





    }
}
