import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class NewUser {

    public static class UserMapper extends Mapper<Object, Text, Text, Text> {

        private Text outKey = new Text();
        private Text outValue = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            // Get the new user data from input
            String newUser = value.toString();
            outKey.set("newUser");
            outValue.set(newUser);
            context.write(outKey, outValue);
        }
    }

    public static class UserReducer extends Reducer<Text, Text, Text, Text> {

        private Text outKey = new Text();
        private Text outValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // Get the new user data and write it to a database
            Configuration config = context.getConfiguration();
            String dbUrl = config.get("dbUrl");
            String dbUser = config.get("dbUser");
            String dbPassword = config.get("dbPassword");

            try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword)) {
                String insertQuery = "INSERT INTO users (user_data) VALUES (?)";
                PreparedStatement pstmt = conn.prepareStatement(insertQuery);

                for (Text value : values) {
                    String newUser = value.toString();
                    pstmt.setString(1, newUser);
                    pstmt.executeUpdate();
                }

                outKey.set("newUserCreated");
                outValue.set("New users added to the database");
                context.write(outKey, outValue);
            } catch (SQLException e) {
                // Handle any database errors
                e.printStackTrace();
            }
        }

    }
}