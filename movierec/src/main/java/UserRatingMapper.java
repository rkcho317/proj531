import java.io.IOException;

public class UserRatingMapper extends Mapper<Object, Text, Text, Text>{
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
