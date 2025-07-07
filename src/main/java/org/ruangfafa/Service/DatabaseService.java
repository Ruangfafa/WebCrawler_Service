package org.ruangfafa.Service;

import java.io.IOException;
import java.io.InputStream;
import java.security.SecureRandom;
import java.sql.*;
import java.util.*;

public class DatabaseService {

    private static String URL;
    private static String USER;
    private static String PASSWORD;
    private static final String WORKDIC = "DatabaseService.java";

    static {
        try (InputStream input = DatabaseService.class.getClassLoader().getResourceAsStream("LocalVars.properties")) {
            Properties props = new Properties();
            if (input != null) {
                props.load(input);
                URL = props.getProperty("db.url");
                USER = props.getProperty("db.user");
                PASSWORD = props.getProperty("db.password");
            } else {
                Logger.log("❌ 找不到 LocalVars.properties", WORKDIC);
                throw new IOException("找不到 LocalVars.properties");
            }
        } catch (IOException e) {
            Logger.log("❌ 配置加载失败: " + e.getMessage(), WORKDIC);
        }
    }

    public static Connection getConnection() {
        try {
            Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
            Logger.log("✅ 成功连接到数据库！", WORKDIC);
            return conn;
        } catch (SQLException e) {
            Logger.log("❌ 无法连接数据库: " + e.getMessage(), WORKDIC);
            return null;
        }
    }

    public static List<String> pullTargetSellers(Connection conn) {
        List<String> sellerUrls = new ArrayList<>();
        String sql =
                "SELECT sellerUrl " +
                        "FROM ServerDB.TargetSellers " +
                        "WHERE DATEDIFF(CURDATE(), lastCraw) >= (" +
                            "SELECT value FROM ServerDB.Config WHERE `key` = 'coolDown'" +
                        ")";
            try (PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                String url = rs.getString("sellerUrl");
                sellerUrls.add(url);
                try (PreparedStatement updateStmt = conn.prepareStatement(
                        "UPDATE ServerDB.TargetSellers SET lastCraw = CURDATE() WHERE sellerUrl = ?")) {
                    updateStmt.setString(1, url);
                    updateStmt.executeUpdate();
                } catch (SQLException updateEx) {
                    Logger.log("⚠️ 更新 lastCraw 失败: " + updateEx.getMessage(), WORKDIC);
                }
            }
        } catch (SQLException e) {
            Logger.log("❌ 查询 TargetSellers 失败: " + e.getMessage(), WORKDIC);
        }
        return sellerUrls;
    }

    public static List<Long> pullClients(Connection conn) {
        List<Long> clientDevices = new ArrayList<>();
        String sqlSchemas = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME LIKE 'ClientDB\\_%' ESCAPE '\\\\'";

        try (PreparedStatement stmtSchemas = conn.prepareStatement(sqlSchemas);
             ResultSet rsSchemas = stmtSchemas.executeQuery()) {

            while (rsSchemas.next()) {
                String schema = rsSchemas.getString(1);

                try (PreparedStatement stmt = conn.prepareStatement(
                        "SELECT state FROM " + schema + ".State")) {
                    try (ResultSet rs = stmt.executeQuery()) {
                        if (rs.next() && rs.getInt("state") == 0) {
                            // 提取 device ID：ClientDB_12345 => 12345
                            String numberPart = schema.substring("ClientDB_".length());
                            try {
                                long device = Long.parseLong(numberPart);
                                clientDevices.add(device);
                            } catch (NumberFormatException e) {
                                Logger.log("⚠️ 无法解析 device ID: " + schema, WORKDIC);
                            }
                        }
                    }
                } catch (SQLException e) {
                    Logger.log("⚠️ 查询 " + schema + ".State 失败: " + e.getMessage(), WORKDIC);
                }
            }
        } catch (SQLException e) {
            Logger.log("❌ pullClients 失败: " + e.getMessage(), WORKDIC);
        }
        return clientDevices;
    }

    public static List<Map<String, String>> pullIdentifierAndPageType(Connection conn, String table) {
        List<Map<String, String>> results = new ArrayList<>();

        String query = "SELECT pageType, identifier FROM ServerDB." + table;
        try (PreparedStatement stmt = conn.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                Map<String, String> row = new HashMap<>();
                row.put("pageType", rs.getString("pageType"));
                row.put("identifier", rs.getString("identifier"));
                results.add(row);
            }

        } catch (SQLException e) {
            System.err.println("查询失败：" + e.getMessage());
        }

        return results;
    }

    public static List<Map<String, String>> pullIdentifierAndPageTypeAndCP(Connection conn, String table) {
        List<Map<String, String>> results = new ArrayList<>();
        String query = "SELECT pageType, identifier, category_pv FROM ServerDB." + table;

        try (PreparedStatement stmt = conn.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                Map<String, String> row = new HashMap<>();
                row.put("pageType", rs.getString("pageType"));
                row.put("identifier", rs.getString("identifier"));
                row.put("category_pv", rs.getString("category_pv"));
                results.add(row);
            }
        } catch (SQLException e) {
            Logger.log("❌ 查询 " + table + " 表失败: " + e.getMessage(), WORKDIC);
        }

        return results;
    }

    public static String createClient(Connection conn) {
        long device = 0;
        boolean exists = true;
        while (exists) {
            device = System.currentTimeMillis() + (long) (Math.random() * 1000);
            if (device == 0) continue;
            try (PreparedStatement checkStmt = conn.prepareStatement(
                    "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ?")) {
                checkStmt.setString(1, "ClientDB_" + device);
                ResultSet rs = checkStmt.executeQuery();
                if (!rs.next()) {
                    exists = false; // 不存在说明可以创建
                }
            } catch (SQLException e) {
                Logger.log("❌ 检查 device 冲突失败: " + e.getMessage(), WORKDIC);
                return "";
            }
        }

        String schemaName = "ClientDB_" + device;
        String username = "WebCrawler_Client" + device;
        String password = generateStrongPassword(14);

        try {
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS " + schemaName);
                stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " + schemaName + ".Task (" +
                        "url VARCHAR(1000) NOT NULL)");
                stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " + schemaName + ".State (" +
                        "state INT)");

                stmt.executeUpdate("INSERT INTO " + schemaName + ".State (state) VALUES (0)");
            }

            conn.commit();
            Logger.log("✅ 创建 Client 成功：device = " + device, WORKDIC);
        } catch (SQLException e) {
            Logger.log("❌ 创建表失败，正在回滚: " + e.getMessage(), WORKDIC);
            try {
                conn.rollback();
            } catch (SQLException rollbackEx) {
                Logger.log("❌ 回滚失败: " + rollbackEx.getMessage(), WORKDIC);
            }
            return "";
        }

        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE USER '" + username + "'@'%' IDENTIFIED BY '" + password + "'");
            stmt.executeUpdate("GRANT SELECT, DELETE ON " + schemaName + ".Task TO '" + username + "'@'%'");
            stmt.executeUpdate("GRANT SELECT, UPDATE ON " + schemaName + ".State TO '" + username + "'@'%'");

            stmt.executeUpdate("GRANT SELECT ON ServerDB.Config TO '" + username + "'@'%'");
            stmt.executeUpdate("GRANT SELECT ON ServerDB.State TO '" + username + "'@'%'");
            // 授权写入公共数据库的部分表
            stmt.executeUpdate("GRANT INSERT ON ServerDB.TargetSellers TO '" + username + "'@'%'");
            stmt.executeUpdate("GRANT INSERT ON ServerDB.TaskRanking TO '" + username + "'@'%'");
            stmt.executeUpdate("GRANT INSERT ON ServerDB.Comment TO '" + username + "'@'%'");
            stmt.executeUpdate("GRANT INSERT ON ServerDB.Sellers TO '" + username + "'@'%'");
            stmt.executeUpdate("GRANT INSERT ON ServerDB.Classificate TO '" + username + "'@'%'");
            stmt.executeUpdate("GRANT INSERT ON ServerDB.ProductTag TO '" + username + "'@'%'");
            stmt.executeUpdate("GRANT INSERT ON ServerDB.Product TO '" + username + "'@'%'");

            stmt.executeUpdate("FLUSH PRIVILEGES");
            Logger.log("✅ 创建用户 " + username + " 成功，密码为: " + password, WORKDIC);
            return password;
        } catch (SQLException e) {
            Logger.log("❌ 创建用户失败，开始清理: " + e.getMessage(), WORKDIC);
            try (Statement cleanupStmt = conn.createStatement()) {
                cleanupStmt.executeUpdate("DROP DATABASE IF EXISTS " + schemaName);
            } catch (SQLException cleanupEx) {
                Logger.log("⚠️ 清理失败（数据库残留）: " + cleanupEx.getMessage(), WORKDIC);
            }
            return "";
        }
    }

    private static String generateStrongPassword(int length) {
        String upper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        String lower = "abcdefghijklmnopqrstuvwxyz";
        String digits = "0123456789";
        String symbols = "!@#$%^&*()-_=+";
        String all = upper + lower + digits + symbols;
        SecureRandom random = new SecureRandom();
        StringBuilder password = new StringBuilder();

        for (int i = 0; i < length; i++) {
            password.append(all.charAt(random.nextInt(all.length())));
        }
        return password.toString();
    }

    public static void deleteClient(Connection conn, Long device) {
        String schemaName = "ClientDB_" + device;
        String username = "WebCrawler_Client" + device;

        try (Statement stmt = conn.createStatement()) {
            try {
                stmt.executeUpdate("DROP DATABASE IF EXISTS " + schemaName);
                Logger.log("✅ 删除数据库 " + schemaName + " 成功", WORKDIC);
            } catch (SQLException e) {
                Logger.log("⚠️ 删除数据库失败: " + e.getMessage(), WORKDIC);
            }

            try {
                stmt.executeUpdate("DROP USER IF EXISTS '" + username + "'@'%'");
                Logger.log("✅ 删除用户 " + username + " 成功", WORKDIC);
            } catch (SQLException e) {
                Logger.log("⚠️ 删除 MySQL 用户失败: " + e.getMessage(), WORKDIC);
            }

            try {
                stmt.executeUpdate("FLUSH PRIVILEGES");
            } catch (SQLException e) {
                Logger.log("⚠️ 刷新权限失败: " + e.getMessage(), WORKDIC);
            }
        } catch (SQLException e) {
            Logger.log("❌ deleteClient 过程失败: " + e.getMessage(), WORKDIC);
        }
    }

    public static void assignUrlToClient(Connection conn, long device, String url) {
        String schemaName = "ClientDB_" + device;
        try (PreparedStatement insertStmt = conn.prepareStatement(
                "INSERT INTO " + schemaName + ".Task (url) VALUES (?)")) {
            insertStmt.setString(1, url);
            insertStmt.executeUpdate();
            Logger.log("✅ 已分配 URL 到 " + schemaName + ".Task: " + url, WORKDIC);
        } catch (SQLException e) {
            Logger.log("❌ 分配 URL 到 " + schemaName + ".Task 失败: " + e.getMessage(), WORKDIC);
        }
    }

    public static boolean setState(Connection conn, long device, int state) {
        String schemaName = (device == 0) ? "ServerDB" : "ClientDB_" + device;
        String sql = "UPDATE " + schemaName + ".State SET state = ?";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, state);
            int affectedRows = stmt.executeUpdate(); // 必须执行更新
            if (affectedRows > 0) {
                Logger.log("✅ 更新 " + schemaName + ".State 状态为 " + state, WORKDIC);
                return true;
            } else {
                Logger.log("⚠️ 未更新任何行，可能 " + schemaName + ".State 表为空", WORKDIC);
                return false;
            }
        } catch (SQLException e) {
            Logger.log("❌ setState 失败: " + e.getMessage(), WORKDIC);
            return false;
        }
    }
    public static int getState(Connection conn, long device) {
        String schemaName = (device == 0) ? "ServerDB" : "ClientDB_" + device;
        String sql = "SELECT state FROM " + schemaName + ".State";

        try (PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                return rs.getInt("state");
            }
        } catch (Exception e) {
            Logger.log("❌ 查询 " + schemaName + ".State 状态失败: " + e.getMessage(), "Application.java");
        }
        return 0; // 默认返回 0 表示“运行中”或“未知状态”
    }
    public static int rebuildTaskProductTable(Connection conn) {
        final String WORKDIC = "DatabaseService.java";

        try (Statement stmt = conn.createStatement()) {
            // 删除 TaskProduct 表
            stmt.executeUpdate("DROP TABLE IF EXISTS ServerDB.TaskProduct");

            // 新建 TaskProduct 表
            stmt.executeUpdate("CREATE TABLE ServerDB.TaskProduct (" +
                    "pageType VARCHAR(255) NOT NULL, " +
                    "id VARCHAR(255) NOT NULL)");

            // 插入唯一组合
            String insertSql = "INSERT INTO ServerDB.TaskProduct (pageType, id) " +
                    "SELECT DISTINCT pageType, id FROM ServerDB.ProductTag " +
                    "WHERE id IS NOT NULL AND id <> ''";
            int affected = stmt.executeUpdate(insertSql);
            Logger.log("✅ 已插入 " + affected + " 条记录到 ServerDB.TaskProduct", WORKDIC);
            return affected;
        } catch (SQLException e) {
            Logger.log("❌ rebuildTaskProductTable 失败: " + e.getMessage(), WORKDIC);
            return -1;
        }
    }


    public static List<Map<String, String>> pullPageTypeAndId(Connection conn) {
        List<Map<String, String>> result = new ArrayList<>();
        String sql = "SELECT pageType, id FROM ServerDB.TaskProduct";

        try (PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                Map<String, String> map = new HashMap<>();
                map.put("pageType", rs.getString("pageType"));
                map.put("id", rs.getString("id"));
                result.add(map);
            }

        } catch (SQLException e) {
            Logger.log("❌ pullPageTypeAndId 查询失败: " + e.getMessage(), WORKDIC);
        }

        return result;
    }
}
