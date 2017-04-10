package by.darashchonak.mentoring.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) {

        Properties dbprops = new Properties();
        //
        // try (InputStream in = new FileInputStream("example.properties")) {
        // dbprops.load(in);
        // } catch (IOException e) {
        // e.printStackTrace();
        // }

        String insert = "INSERT INTO test (data) VALUES (?)";

        try (Connection connection = DriverManager.getConnection("jdbc:mysql://192.168.100.6/my", "root", "root");
                PreparedStatement statement = connection.prepareStatement(insert);

        ) {

            for (int j = 0; j < 100000; j++) {
                statement.setString(1, "dasda" + j);
                statement.executeUpdate();
            }

        } catch (SQLException e) {

            e.printStackTrace();
        }
    }
}
