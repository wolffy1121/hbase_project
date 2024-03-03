package com.wolffy.hbase.ch1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Hbase_02_DML {
    public static void main(String[] args) throws Exception {
        putData("test", "1001", "cf1", "cl1", "value1");

    }
    //TODO 插入数据
    public static void putData(String tableName, String rowKey, String cf, String cn, String value) throws IOException {

        //1.获取配置信息并设置连接参数
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

        //2.获取连接
        Connection connection = ConnectionFactory.createConnection(configuration);

        //3.获取表的连接
        Table table = connection.getTable(TableName.valueOf(tableName));

        //4.创建Put对象
        Put put = new Put(Bytes.toBytes(rowKey));

        //5.放入数据
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(value));

        //6.执行插入数据操作
        table.put(put);

        //7.关闭连接
        table.close();
        connection.close();
    }

    //TODO 单条数据查询(GET)
    public static void getDate(String tableName, String rowKey, String cf, String cn) throws IOException {

        //1.获取配置信息并设置连接参数
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

        //2.获取连接
        Connection connection = ConnectionFactory.createConnection(configuration);

        //3.获取表的连接
        Table table = connection.getTable(TableName.valueOf(tableName));

        //4.创建Get对象
        Get get = new Get(Bytes.toBytes(rowKey));
        // 指定列族查询
        // get.addFamily(Bytes.toBytes(cf));
        // 指定列族:列查询
        // get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));

        //5.查询数据
        Result result = table.get(get);

        //6.解析result
        for (Cell cell : result.rawCells()) {
            System.out.println("ROW:" + Bytes.toString(CellUtil.cloneRow(cell)) +
                    " CF:" + Bytes.toString(CellUtil.cloneFamily(cell))+
                    " CL:" + Bytes.toString(CellUtil.cloneQualifier(cell))+
                    " VALUE:" + Bytes.toString(CellUtil.cloneValue(cell)));
        }

        //7.关闭连接
        table.close();
        connection.close();

    }
    //TODO 扫描数据(Scan)
    public static void scanTable(String tableName) throws IOException {

        //1.获取配置信息并设置连接参数
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

        //2.获取连接
        Connection connection = ConnectionFactory.createConnection(configuration);

        //3.获取表的连接
        Table table = connection.getTable(TableName.valueOf(tableName));

        //4.创建Scan对象
        Scan scan = new Scan();

        //5.扫描数据
        ResultScanner results = table.getScanner(scan);

        //6.解析results
        for (Result result : results) {
            for (Cell cell : result.rawCells()) {
                System.out.println(
                        Bytes.toString(CellUtil.cloneRow(cell))+":"+
                                Bytes.toString(CellUtil.cloneFamily(cell))+":" +
                                Bytes.toString(CellUtil.cloneQualifier(cell)) +":" +
                                Bytes.toString(CellUtil.cloneValue(cell))
                );
            }
        }

        //7.关闭资源
        table.close();
        connection.close();

    }

    //TODO 删除数据
    public static void deletaData(String tableName, String rowKey, String cf, String cn) throws IOException {

        //1.获取配置信息并设置连接参数
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

        //2.获取连接
        Connection connection = ConnectionFactory.createConnection(configuration);

        //3.获取表的连接
        Table table = connection.getTable(TableName.valueOf(tableName));

        //4.创建Delete对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));

        // 指定列族删除数据
        // delete.addFamily(Bytes.toBytes(cf));
        // 指定列族:列删除数据(所有版本)
        // delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
        // 指定列族:列删除数据(指定版本)
        // delete.addColumns(Bytes.toBytes(cf), Bytes.toBytes(cn));

        //5.执行删除数据操作
        table.delete(delete);

        //6.关闭资源
        table.close();
        connection.close();

    }
}
