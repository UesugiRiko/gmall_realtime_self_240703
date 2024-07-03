package com.atguigu.realtime.util;

import com.atguigu.realtime.common.Constant;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class FlinkJdbcUtil {
    public static Connection getJdbcConnection(String driver, String url, String username, String password) {
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("驱动错误" + driver);
        }
        try {
            return DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            throw new RuntimeException("错误！url=" + url + ",username=" + username + ",password=" + password);
        }
    }

    public static Connection getPhoenixConnection() {
        return getJdbcConnection(Constant.PHOENIX_DRIVER, Constant.PHOENIX_URL, null, null);
    }

    public static void closePhoenixConnection(Connection conn) {
        closeJdbcConnection(conn);
    }

    public static void closeJdbcConnection(Connection conn) {
        try {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
