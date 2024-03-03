package com.wolffy.hbase.ch2;

import java.sql.*;
import java.util.Properties;

public class thickPhoenix {
    public static void main(String[] args) throws SQLException {
        String url = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";
        Properties props = new Properties();
        props.put("phoenix.schema.isNamespaceMappingEnabled", "true");
        Connection connection = DriverManager.getConnection(url, props);
        PreparedStatement ps = connection.prepareStatement("select * from student");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getString(1) + ":" + rs.getString(2));
        }
        connection.close();
    }
}
