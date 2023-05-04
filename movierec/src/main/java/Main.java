import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.sql.*;

public class MovieRecommendation {

    public static class UserRatingMapper extends Mapper<Object, Text, Text, Text> {

        private Text outKey = new Text();
        private Text outValue = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            String userId = tokens[0];
            String movieId = tokens[1];
            String rating = tokens[2];
            outKey.set(userId);
            outValue.set(movieId + ":" + rating);
            context.write(outKey, outValue);
        }
    }

    public static class UserRatingReducer extends Reducer<Text, Text, Text, Text> {

        private Text outKey = new Text();
        private Text outValue = new Text();
        private Map<String, Float> movieRatings = new HashMap<>();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                String[] tokens = value.toString().split(":");
                String movieId = tokens[0];
                float rating = Float.parseFloat(tokens[1]);
                if (movieRatings.containsKey(movieId)) {
                    rating = (rating + movieRatings.get(movieId)) / 2.0f;
                }
                movieRatings.put(movieId, rating);
            }

            List<Map.Entry<String, Float>> list = new ArrayList<>(movieRatings.entrySet());
            list.sort(Map.Entry.comparingByValue());

            String recommendedMovies = "";
            int count = 0;
            for (Map.Entry<String, Float> entry : list) {
                recommendedMovies += entry.getKey() + ",";
                count++;
                if (count >= 5) {
                    break;
                }
            }
            outKey.set(key);
            outValue.set(recommendedMovies);
            context.write(outKey, outValue);
            movieRatings.clear();
        }
    }

    public class Main {
        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "movie recommendation");
            job.setJarByClass(MovieRecommendation.class);
            job.setMapperClass(UserRatingMapper.class);
            job.setReducerClass(UserRatingReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }
}