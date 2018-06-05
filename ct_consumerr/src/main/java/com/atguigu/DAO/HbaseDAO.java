package com.atguigu.DAO;

import com.atguigu.utils.ConnectionInstance;
import com.atguigu.utils.HbaseUtil;
import com.atguigu.utils.PropertyUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import javax.swing.plaf.basic.BasicListUI;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;


public class HbaseDAO {
    private String namespace;
    private String tableName;
    private int regions;
    private String cf;
    private HTable table;
    private String flag;
    private SimpleDateFormat sdf = null;
    //缓存put对象的集合
    private List<Put> listPut;

    public HbaseDAO() throws IOException {
        //初始化相关属性（数据来源于配置文件kafka_hbase.properties）
        namespace = PropertyUtil.properties.getProperty("hbase.namespace");
        tableName = PropertyUtil.properties.getProperty("hbase.table.name");
        regions = Integer.valueOf(PropertyUtil.properties.getProperty("hbase.regions"));
        cf = PropertyUtil.properties.getProperty("hbase.table.cf");
        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        listPut = new ArrayList<>();
        flag = "1";
        //初始化命名空间及表的创建
        if (!HbaseUtil.isTableExist(tableName)) {
            HbaseUtil.initNamespace(namespace);
            HbaseUtil.createTable(tableName, regions, cf, "f2");
        }
    }

    public void put(String ori) throws IOException, ParseException, ParseException {

        //创建连接及获取表
        if (listPut.size() == 0) {
            //获取连接（单例对象）
            Connection connection = ConnectionInstance.getInstance();
            //获取表对象
            table = (HTable) connection.getTable(TableName.valueOf(tableName));
            //设置不自动提交
            table.setAutoFlushTo(false);
            //设置客户端缓存大小
            table.setWriteBufferSize(1024 * 1024);
        }

        //如果传输的数据为空直接返回
        if (ori == null) return;

        //ori:14314302040,19460860743,2019-05-08 23:41:05,0439
        String[] split = ori.split(",");//切分原始数据

        //截取字段封装相关参数
        String caller = split[0];//主叫
        String callee = split[1];//被叫
        String buildTime = split[2];//通话建立时间
        long time = sdf.parse(buildTime).getTime();//通话建立时间戳
        String buildtime_ts = time + "";//时间戳转换为string类型
        String duration = split[3];//通话时长

        //获取分区号
        String regionHash = HbaseUtil.getRegionHash(caller, buildTime, regions);

        //获取rowkey：regionHash_caller_buildTime_callee_duration
        String rowKey = HbaseUtil.getRowKey(regionHash, caller, buildTime, callee, flag, duration);

        //为每一条数据创建put对象
        Put put = new Put(Bytes.toBytes(rowKey));

        //向put中添加数据（列族：列）（值）
        //call1,buildtime,buildtime_ts,call2,duration
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("call1"), Bytes.toBytes(caller));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("buildtime"), Bytes.toBytes(buildTime));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("buildtime_ts"), Bytes.toBytes(buildtime_ts));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("call2"), Bytes.toBytes(callee));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("flag"), Bytes.toBytes(flag));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("duration"), Bytes.toBytes(duration));

        //向put缓存中添加对象
        listPut.add(put);

        //当list中数据条数达到20条，则写入HBase
        if (listPut.size() > 20) {
            table.put(listPut);
            //手动提交
            table.flushCommits();
            //清空list集合
            listPut.clear();
            //关闭表连接（如果业务单一，可以初始化时创建表连接，此处就不需要关闭表连接）
            table.close();
        }

    }
}