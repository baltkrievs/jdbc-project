package by.darashchonak.mentoring.jdbc;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class App {

    private static int BATCH_SIZE = 1000;

    public static void main(String[] args) {

        try (Connection fromConn = DriverManager.getConnection("jdbc:mysql://192.168.100.5/from", "root", "root");
                Connection toConn = DriverManager.getConnection("jdbc:mysql://192.168.100.5/to", "root", "root")) {

            // String insert1 = "INSERT INTO new_table (data) VALUES (" +
            // UUID.randomUUID().toString() + ");";
            PreparedStatement insert1 = fromConn.prepareStatement("INSERT INTO new_table (data) VALUES (?);");

            for (int j = 0; j < 100; j++) {
                for (int i = 0; i < 1000; i++) {
                    insert1.setString(1, UUID.randomUUID().toString());
                    insert1.addBatch();
                }
                insert1.executeBatch();
            }

            // List<String> tables = getTables(fromConn);
            // for (String table : tables) {
            //
            // String sql = buildCreateTableSQL(table, fromConn);
            //
            // if (isTableExists(table, toConn)) {
            // dropTable(table, toConn);
            // }
            //
            // PreparedStatement statement = toConn.prepareStatement(sql);
            // statement.execute();
            // statement.close();
            // }
            //
            // Collections.sort(tables);
            //
            // for (String tableName : tables) {
            // PreparedStatement query = fromConn.prepareStatement("SELECT *
            // FROM " + tableName);
            // query.setFetchSize(BATCH_SIZE);
            // ResultSet result = query.executeQuery();
            // ResultSetMetaData rsmd = result.getMetaData();
            //
            // System.out.println(rsmd.getColumnCount());
            //
            // PreparedStatement insert =
            // toConn.prepareStatement(buildInsertSql(tableName, fromConn));
            //
            // int count = 0;
            // while (result.next()) {
            // for (int j = 1; j <= rsmd.getColumnCount(); j++) {
            // insert.setObject(j, result.getObject(j));
            // }
            // insert.addBatch();
            //
            // if (++count % BATCH_SIZE == 0) {
            // insert.executeBatch();
            // count = 0;
            // }
            // }
            // insert.executeBatch();
            // }

        } catch (SQLException e) {

            e.printStackTrace();
        }
    }

    private static List<String> getTables(Connection connection) throws SQLException {
        List<String> tables = new ArrayList<>();

        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet rs = metaData.getTables(connection.getCatalog(), connection.getSchema(), "%",
                new String[] { "TABLE" });

        while (rs.next()) {
            String tableName = rs.getString(3);

            System.out.println(tableName);
            tables.add(tableName);
        }

        return tables;
    }

    private static String buildCreateTableSQL(String tableName, Connection connection) throws SQLException {

        StringBuilder sql = new StringBuilder("CREATE TABLE " + tableName + " (");

        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet resultSet = metaData.getColumns(connection.getCatalog(), connection.getSchema(), tableName, "");

        while (resultSet.next()) {
            sql.append(resultSet.getString(4));
            sql.append(" ");
            sql.append(sqlType(resultSet.getInt(5)));
            sql.append("(" + resultSet.getInt(7) + ")");
            if (!resultSet.isLast()) {
                sql.append(", ");
            }
        }

        sql.append(");");

        return sql.toString();

    }

    private static String sqlType(int type) {

        Class<Types> typezz = Types.class;

        for (Field field : typezz.getDeclaredFields()) {
            try {
                if ((int) field.get(null) == type) {
                    return field.getName();
                }
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private static boolean isTableExists(String tableName, Connection connection) throws SQLException {

        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet rs = metaData.getTables(connection.getCatalog(), connection.getSchema(), tableName, null);

        while (rs.next()) {
            String tName = rs.getString(3);
            if (tName != null && tName.equals(tableName)) {
                return true;
            }
        }
        return false;
    }

    private static void dropTable(String tableName, Connection connection) throws SQLException {

        PreparedStatement dropTable = connection.prepareStatement("DROP TABLE " + tableName);
        dropTable.execute();
    }

    private static String buildInsertSql(String tableName, Connection connection) throws SQLException {

        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet rs = metaData.getColumns(connection.getCatalog(), connection.getSchema(), tableName, "%");

        List<String> columns = new ArrayList<>();
        List<String> values = new ArrayList<>();

        while (rs.next()) {
            columns.add(rs.getString(4));
            values.add("?");
        }

        String insert = "INSERT INTO " + tableName + " (" + String.join(",", columns) + ") VALUES ("
                + String.join(",", values) + ");";

        System.out.println(insert);

        return insert;
    }

}
