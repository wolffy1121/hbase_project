package com.wolffy.hbase.ch2;


import org.apache.phoenix.queryserver.client.ThinClientUtil;

import java.sql.*;


public class thinPhoenix {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {

        // 1. 直接从瘦客户端获取链接
        String hadoop102 = ThinClientUtil.getConnectionUrl("hadoop102", 8765);
        // 2. 获取连接
        Connection connection = DriverManager.getConnection(hadoop102);

        // 3.编译SQL语句
        PreparedStatement preparedStatement = connection.prepareStatement("select * from student");

        // 4.执行语句
        ResultSet resultSet = preparedStatement.executeQuery();

        // 5.输出结果
        while (resultSet.next()) {
            System.out.println(resultSet.getString(1) + ":" + resultSet.getString(2) + ":" + resultSet.getString(3));
        }

        // 6.关闭资源
        connection.close();
    }
}
