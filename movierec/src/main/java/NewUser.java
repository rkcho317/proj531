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
            FileSystem fs = FileSystem.get(config);
            Path dbPath = new Path(config.get("dbPath"));
            Path newUserPath = new Path(dbPath, "new_user.txt");
            String newUser = "";
            for (Text value : values) {
                newUser = value.toString();
            }
            fs.create(newUserPath);
            fs.append(newUserPath, newUser.getBytes(), newUser.getBytes().length);
            outKey.set("newUserCreated");
            outValue.set(newUser);
            context.write(outKey, outValue);
        }
    }

    /* public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.set("dbPath", args[0]);

        Job job = Job.getInstance(config, "NewUser");
        job.setJarByClass(NewUser.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(UserMapper.class);
        job.setReducerClass(UserReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.addInputPath(job, new Path(args[1]));
        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    } */
}
