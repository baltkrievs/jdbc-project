package by.darashchonak.mentoring.jdbc;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class DataTool {

    private final static int BATCH_SIZE = 1000;
    private final Properties srcProps = new Properties();
    private final Properties dstProps = new Properties();

    private Connection srcConnection = null;
    private Connection dstConnection = null;

    public DataTool() {
        try (FileInputStream srcInputStream = new FileInputStream(new File("src.properties"));
             FileInputStream dstInputStream = new FileInputStream(new File("dst.properties"))) {

            srcProps.load(srcInputStream);
            dstProps.load(dstInputStream);
        } catch (IOException e) {
            System.out.println("Could not find file src.properties or dst.properties");
        }

        srcConnection = getConnection(Direction.SRC);
        dstConnection = getConnection(Direction.DST);
    }


    public List<String> getTables() {

        List<String> tables = new ArrayList<>();

        try {
            DatabaseMetaData metaData = srcConnection.getMetaData();
            ResultSet rs = metaData.getTables(srcConnection.getCatalog(), srcConnection.getSchema(), "%",
                    new String[]{"TABLE"});

            while (rs.next()) {
                String tableName = rs.getString(3);
                tables.add(tableName);
            }
        } catch (SQLException e){
            e.printStackTrace();
        }


        return tables;
    }

    public void createTables(List<String> tables) {
        for (String table : tables) {

            try {
                String sql = buildCreateTableSQL(table);

                if (isTableExists(table)) {
                    dropTable(table);
                }

                PreparedStatement statement = dstConnection.prepareStatement(sql);
                statement.execute();
                statement.close();

            } catch (SQLException e){
                e.printStackTrace();
            }
        }
    }


    public void copyTable(String tableName, boolean reverse) {

        try {

            Statement query = srcConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            query.setFetchSize(Integer.MIN_VALUE);
            ResultSet result = query.executeQuery("SELECT * FROM " + tableName);
            ResultSetMetaData rsmd = result.getMetaData();

            PreparedStatement insert =
                    dstConnection.prepareStatement(buildInsertSql(tableName, srcConnection));

            int count = 0;

            if (reverse) {
                result.last();
            }

            while (hasNext(result, reverse)) {
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

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private Connection getConnection(Direction direction) {

        Connection connection = null;

        try {
            if (direction.equals(Direction.SRC)) {
                connection = DriverManager.getConnection(srcProps.getProperty("db.url"), srcProps);
            } else {
                connection = DriverManager.getConnection(dstProps.getProperty("db.url"), dstProps);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }


    private String buildCreateTableSQL(String tableName) throws SQLException {

        StringBuilder sql = new StringBuilder("CREATE TABLE " + tableName + " (");

        DatabaseMetaData metaData = srcConnection.getMetaData();
        ResultSet resultSet = metaData.getColumns(srcConnection.getCatalog(), srcConnection.getSchema(), tableName, "");

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

    private boolean isTableExists(String tableName) throws SQLException {

        DatabaseMetaData metaData = dstConnection.getMetaData();
        ResultSet rs = metaData.getTables(dstConnection.getCatalog(), dstConnection.getSchema(), tableName, null);

        while (rs.next()) {
            String tName = rs.getString(3);
            if (tName != null && tName.equals(tableName)) {
                return true;
            }
        }
        return false;
    }

    private void dropTable(String tableName) throws SQLException {

        PreparedStatement dropTable = dstConnection.prepareStatement("DROP TABLE " + tableName);
        dropTable.execute();
    }

    private String buildInsertSql(String tableName, Connection connection) throws SQLException {

        // Have to obtain new connection here cuz seems like metadata object doesn't work if query is executed
        srcConnection = getConnection(Direction.SRC);

        DatabaseMetaData metaData = srcConnection.getMetaData();
        ResultSet rs = metaData.getColumns(connection.getCatalog(), connection.getSchema(), tableName, "%");

        List<String> columns = new ArrayList<>();
        List<String> values = new ArrayList<>();

        while (rs.next()) {
            columns.add(rs.getString(4));
            values.add("?");
        }

        return "INSERT INTO " + tableName + " (" + String.join(",", columns) + ") VALUES ("
                + String.join(",", values) + ");";
    }

    private boolean hasNext(ResultSet resultSet, boolean reverse) {
        boolean isHasNext = false;

        try {
            if (reverse) {
                isHasNext = resultSet.previous();
            } else {
                isHasNext = resultSet.next();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return isHasNext;
    }
}
