import java.io.IOException;
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


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
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

    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {

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

    public static class MovieReducer extends Reducer<Text, Text, Text, Text> {

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

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("movie_recommendation").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().appName("movie_recommendation").getOrCreate();

        Dataset<Row> name_basics_df = spark.read()
                .option("header", true)
                .option("sep", "\t")
                .option("inferSchema", true)
                .csv("name.basics.tsv.gz");

        Dataset<Row > title_akas_df = spark.read()
                .option("header", true)
                .option("sep", "\t")
                .option("inferSchema", true)
                .csv("title.akas.tsv.gz");

        Dataset<Row> title_basics_df = spark.read()
                .option("header", true)
                .option("sep", "\t")
                .option("inferSchema", true)
                .csv("title.basics.tsv.gz");

        Dataset<Row> title_crew_df = spark.read()
                .option("header", true)
                .option("sep", "\t")
                .option("inferSchema", true)
                .csv("title.crew.tsv.gz");

        Dataset<Row> title_episode_df = spark.read()
                .option("header", true)
                .option("sep", "\t")
                .option("inferSchema", true)
                .csv("title.episode.tsv.gz");

        Dataset<Row> title_principals_df = spark.read()
                .option("header", true)
                .option("sep", "\t")
                .option("inferSchema", true)
                .csv("title.principals.tsv.gz");

        Dataset<Row> title_ratings_df = spark.read()
                .option("header", true)
                .option("sep", "\t")
                .option("inferSchema", true)
                .csv("title.ratings.tsv.gz");

        name_basics_df.show();
        title_akas_df.show();
        title_basics_df.show();
        title_crew_df.show();
        title_episode_df.show();
        title_principals_df.show();
        title_ratings_df.show();

        job.setJarByClass(MovieRecommender.class);
        job.setMapperClass(MovieMapper.class);
        job.setReducerClass(MovieReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        // Create a Hadoop job configuration
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Recommender");

        // Set the classes for the job
        job.setJarByClass(MovieRecommender.class);
        job.setMapperClass(MovieRecommender.UserMapper.class);
        job.setReducerClass(MovieRecommender.UserReducer.class);

        // Set the output key and value classes for the mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Set the output key and value classes for the reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set the input and output paths
        FileInputFormat.addInputPath(job, new Path("input/user_data.txt"));
        FileOutputFormat.setOutputPath(job, new Path("output/temp"));

        // Execute the job and wait for completion
        job.waitForCompletion(true);

        // Create a new job for movie recommendation
        Job recommendationJob = Job.getInstance(conf, "Movie Recommendation");

        // Set the classes for the recommendation job
        recommendationJob.setJarByClass(MovieRecommender.class);
        recommendationJob.setMapperClass(MovieRecommender.MovieMapper.class);
        recommendationJob.setReducerClass(MovieRecommender.MovieReducer.class);

        // Set the output key and value classes for the mapper
        recommendationJob.setMapOutputKeyClass(Text.class);
        recommendationJob.setMapOutputValueClass(Text.class);

        // Set the output key and value classes for the reducer
        recommendationJob.setOutputKeyClass(Text.class);
        recommendationJob.setOutputValueClass(Text.class);

        // Set the input and output paths
        FileInputFormat.addInputPath(recommendationJob, new Path("input/movie_data.txt"));
        FileOutputFormat.setOutputPath(recommendationJob, new Path("output/recommendations"));

        // Execute the recommendation job and wait for completion
        recommendationJob.waitForCompletion(true);
    }
    }
