import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// Goal:
// -For each movie, output the column numbers of the of users who gave the highest ratings
// -If the highest rating is given by multiple users, output all of them in ascending numerical order
// -Assume that there is at least one non-blank rating for each movie

// NOTE:
// There is no need for a reducer here, as each record can be iterated over once to get the result (directly from Mapper)

public class Task1 {
  public static class Task1Mapper extends Mapper<Object, Text, Text, Text> {
    private Text result;
    private Text movieTitle = new Text();
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] tokens = value.toString().split(",");
      result = new Text();
      movieTitle.set(tokens[0]);

      int maxRating = 1;
      // find maxRating for the movie
      for (int i = 1; i < tokens.length; i++) {
        if (!tokens[i].equals(null) && !tokens[i].equals("")) {
          try {
            int tmp = Integer.parseInt(tokens[i]);
            if (tmp > maxRating) {
              maxRating = tmp;
              if (tmp == 5) break; // highest max rating possible is 5
            }
          } catch (NumberFormatException e) {
            continue;
          }
        }
      }

      // find all user columns with the maxRating
      for (int i = 1; i < tokens.length; i++) {
        if (!tokens[i].equals(null) && !tokens[i].equals("")) {
          try {
            int tmp = Integer.parseInt(tokens[i]);
            if (tmp == maxRating) {
              if (result.getLength() != 0) {
                result.append(",".getBytes(StandardCharsets.UTF_8), 0, 1);
              }
              result.append(Integer.toString(i).getBytes(StandardCharsets.UTF_8), 0, Integer.toString(i).length());
            }
          } catch (NumberFormatException e) {
            continue;
          }
        }
      }

      context.write(movieTitle, result);
    }
  }   
    
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");
    
    Job job = Job.getInstance(conf, "Task1");
    job.setJarByClass(Task1.class);

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    
    job.setMapperClass(Task1Mapper.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setNumReduceTasks(0);

    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
