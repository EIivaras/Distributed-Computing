import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// For each user, output the user's column number and the total number of ratings for that user
// If a user has no ratings, then output 0 for that user
// Ex/ 1,2 // user 1 has 2 ratings

public class Task3 {

  public static class Task3Mapper extends Mapper<Object, Text, Text, IntWritable> {
    private static final IntWritable one = new IntWritable(1);
    private static final IntWritable zero = new IntWritable(0);
    Text outputKey = new Text("");
  
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] tokens = value.toString().split(",", -1);
      for (int i = 1; i < tokens.length; i++) {
        outputKey.set(Integer.toString(i));
        if (!tokens[i].equals(null) && !tokens[i].equals("")) {
          context.write(outputKey, one);
        } else {
          context.write(outputKey, zero);
        }
      }
    }
  } 

  public static class Task3Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable result = new IntWritable(0);

    public void reduce(Text user, Iterable<IntWritable> userRated, Context context) throws IOException, InterruptedException {
      result.set(0);
      
      for (IntWritable rated : userRated) {
          result.set(result.get() + rated.get());
      }
      context.write(user, result);
    }
  } 
    
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");
    
    Job job = Job.getInstance(conf, "Task3");
    job.setJarByClass(Task3.class);

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    // add code here
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(Task3Mapper.class);
    job.setCombinerClass(Task3Reducer.class);
    job.setReducerClass(Task3Reducer.class);

    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
