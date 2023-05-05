import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UserRatingReducer extends Reducer<Text, Text, Text, Text>{

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
