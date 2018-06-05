package com.atguigu.utils;

import jdk.nashorn.internal.ir.GetSplitState;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyUtil {
public static Properties properties;
static{
    InputStream is = ClassLoader.getSystemResourceAsStream("kafka_hbase.properties");

    try {
        properties = new Properties();
        properties.load(is);
    } catch (IOException e) {
        e.printStackTrace();
    }

}

}
