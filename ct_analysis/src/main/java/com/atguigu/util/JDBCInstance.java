package com.atguigu.util;

import org.apache.hadoop.hbase.client.Connection;

public class JDBCInstance {
    private static Connection connection=null;

    public JDBCInstance() {
    }
    public static Connection getInstance(){
        if (connection==null||connection.isClosed()){

        }

        return null;
    }
}
