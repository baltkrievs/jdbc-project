package by.darashchonak.mentoring.jdbc;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.*;


public class App {

    private static int BATCH_SIZE = 10;
    private static Properties prop = new Properties();

    public static void main(String[] args) {

        try (FileInputStream fileInputStream = new FileInputStream(new File("db.properties"))) {
            prop.load(fileInputStream);
        } catch (IOException e) {
            System.out.println("Could not find file db.properties");
        }

        try (Connection fromConn = getConnection(Direction.SRC);
             Connection toConn = getConnection(Direction.DST)) {

            List<String> tables = getTables(fromConn);
            for (String table : tables) {

                String sql = buildCreateTableSQL(table, fromConn);

                if (isTableExists(table, toConn)) {
                    dropTable(table, toConn);
                }

                PreparedStatement statement = toConn.prepareStatement(sql);
                statement.execute();
                statement.close();
            }

            Collections.sort(tables);

            for (String tableName : tables) {

                Statement query = fromConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                query.setFetchSize(Integer.MIN_VALUE);
                ResultSet result = query.executeQuery("SELECT * FROM " + tableName);
                ResultSetMetaData rsmd = result.getMetaData();

                PreparedStatement insert =
                        toConn.prepareStatement(buildInsertSql(tableName, fromConn));

                int count = 0;

                if (args.length >= 1 && args[0] != null && args[0].toLowerCase().equals("r")){
                    result.last();
                    while (result.previous()) {
                        for (int j = 1; j <= rsmd.getColumnCount(); j++) {
                            insert.setObject(j, result.getObject(j));
                        }
                        insert.addBatch();

                        if (++count % BATCH_SIZE == 0) {
                            insert.executeBatch();
                            insert.clearParameters();
                        }
                    }
                    insert.executeBatch();
                } else {

                    while (result.next()) {
                        for (int j = 1; j <= rsmd.getColumnCount(); j++) {
                            insert.setObject(j, result.getObject(j));
                        }
                        insert.addBatch();

                        if (++count % BATCH_SIZE == 0) {
                            insert.executeBatch();
                            insert.clearParameters();
                        }
                    }
                    insert.executeBatch();
                }

            }

        } catch (SQLException e) {

            e.printStackTrace();
        }
    }

    private static List<String> getTables(Connection connection) throws SQLException {
        List<String> tables = new ArrayList<>();

        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet rs = metaData.getTables(connection.getCatalog(), connection.getSchema(), "%",
                new String[]{"TABLE"});

        while (rs.next()) {
            String tableName = rs.getString(3);

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

        DatabaseMetaData metaData = getConnection(Direction.SRC).getMetaData();
        ResultSet rs = metaData.getColumns(connection.getCatalog(), connection.getSchema(), tableName, "%");

        List<String> columns = new ArrayList<>();
        List<String> values = new ArrayList<>();

        while (rs.next()) {
            columns.add(rs.getString(4));
            values.add("?");
        }

        String insert = "INSERT INTO " + tableName + " (" + String.join(",", columns) + ") VALUES ("
                + String.join(",", values) + ");";

        return insert;
    }

    private static Connection getConnection(Direction direction) throws SQLException {

        if (direction == Direction.SRC){
            return DriverManager.getConnection("jdbc:mysql://" +
                            prop.getProperty("db.source.host") + "/" +
                            prop.getProperty("db.source.database"),
                    prop.getProperty("db.source.username"),
                    prop.getProperty("db.source.password"));
        } else {
            return DriverManager.getConnection("jdbc:mysql://" +
                            prop.getProperty("db.destination.host") + "/" +
                            prop.getProperty("db.destination.database"),
                    prop.getProperty("db.destination.username"),
                    prop.getProperty("db.destination.password"));
        }
    }

}
