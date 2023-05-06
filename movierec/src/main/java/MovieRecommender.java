import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class MovieRecommender {
    public static class UserMapper extends Mapper<Object, Text, Text, Text> {

        private Text outKey = new Text();
        private Text outValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Parse the user input data
            String[] tokens = value.toString().split(",");
            String favoriteGenre = tokens[0];
            String favoriteDirector = tokens[1];
            String favoriteActor = tokens[2];
            String favoriteYear = tokens[3];

            // Emit the user preferences
            outKey.set("user");
            outValue.set(favoriteGenre + "," + favoriteDirector + "," + favoriteActor + "," + favoriteYear);
            context.write(outKey, outValue);
        }
    }

    public static class UserReducer extends Reducer<Text, Text, Text, Text> {

        private Text outKey = new Text();
        private Text outValue = new Text();
        private Set<String> favoriteGenres = new HashSet<>();
        private Set<String> favoriteDirectors = new HashSet<>();
        private Set<String> favoriteActors = new HashSet<>();
        private Set<Integer> favoriteYears = new HashSet<>();

        private Set<String> favoriteTitles = new HashSet<>();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                String[] tokens = value.toString().split(",");
                favoriteTitles.add(tokens[0]);
                favoriteGenres.add(tokens[1]);
                favoriteDirectors.add(tokens[2]);
                favoriteActors.add(tokens[3]);
                favoriteYears.add(Integer.parseInt(tokens[4]));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Pass the user preferences to the next job
            outKey.set("user");
            outValue.set(String.join(",", favoriteTitles) + "\t" +String.join(",", favoriteGenres) + "\t" + String.join(",", favoriteDirectors) +
                    "\t" + String.join(",", favoriteActors) + "\t" + String.join(",", (CharSequence) favoriteYears));
            context.write(outKey, outValue);
        }
    }

    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {

        private Text outKey = new Text();
        private Text outValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Parse the movie data
            String[] tokens = value.toString().split(",");
            String movieId = tokens[0];
            String genre = tokens[1];
            String director = tokens[2];
            String actor = tokens[3];
            int year = Integer.parseInt(tokens[4]);

            // Emit the movie details
            outKey.set("movie");
            outValue.set(movieId + "," + genre + "," + director + "," + actor + "," + year);
            context.write(outKey, outValue);
        }
    }

    public static class MovieReducer extends Reducer<Text, Text, Text, Text> {

        private Text outKey = new Text();
        private Text outValue = new Text();

        private List<Movie> movieDatabase = new ArrayList<>();
        private Set<String> favoriteGenres = new HashSet<>();
        private Set<String> favoriteDirectors = new HashSet<>();
        private Set<String> favoriteActors = new HashSet<>();
        private Set<Integer> favoriteYears = new HashSet<>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            if (key.toString().equals("user")) {
                // Process user preferences
                for (Text value : values) {
                    String[] preferences = value.toString().split("\t");
                    favoriteGenres.addAll(Arrays.asList(preferences[0].split(",")));
                    favoriteDirectors.addAll(Arrays.asList(preferences[1].split(",")));
                    favoriteActors.addAll(Arrays.asList(preferences[2].split(",")));
                    for (String year : preferences[3].split(",")) {
                        favoriteYears.add(Integer.parseInt(year));
                    }
                }
            } else if (key.toString().equals("movie")) {
                // Process movie details
                for (Text value : values) {
                    String[] tokens = value.toString().split(",");
                    String movieId = tokens[0];
                    String genre = tokens[1];
                    String director = tokens[2];
                    String actor = tokens[3];
                    int year = Integer.parseInt(tokens[4]);
                    movieDatabase.add(new Movie(movieId, genre, director, actor, year));
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Filter movies based on user preferences and generate recommendations
            List<Movie> recommendations = new ArrayList<>();

            for (Movie movie : movieDatabase) {
                if (favoriteGenres.contains(movie.getGenre())

                        && favoriteDirectors.contains(movie.getDirector())
                        && favoriteActors.contains(movie.getActor())
                        && favoriteYears.contains(movie.getYear())) {
                    recommendations.add(movie);
                }
            }

            // Sort the recommendations based on some criteria (e.g., rating, popularity)
            Collections.sort(recommendations, new MovieComparator());

            // Get the top 10 recommendations
            List<Movie> topRecommendations = recommendations.subList(0, Math.min(10, recommendations.size()));

            // Emit the recommendations
            for (Movie movie : topRecommendations) {
                outKey.set(movie.getId());
                outValue.set(movie.getTitle());
                context.write(outKey, outValue);
            }
        }
    }

    public static class Movie {
        private String id;
        private String genre;
        private String director;
        private String actor;
        private int year;

        public Movie(String id, String genre, String director, String actor, int year) {
            this.id = id;
            this.genre = genre;
            this.director = director;
            this.actor = actor;
            this.year = year;
        }

        public String getId() {
            return id;
        }

        public String getGenre() {
            return genre;
        }

        public String getDirector() {
            return director;
        }

        public String getActor() {
            return actor;
        }

        public int getYear() {
            return year;
        }
    }

    public static class MovieComparator implements Comparator<Movie> {
        @Override
        public int compare(Movie m1, Movie m2) {
            // Implement your own criteria for sorting movies
            // For example, you can sort by rating or popularity
            return 0;
        }
    }
}
