import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageCalculator {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, FloatWritable> {

    private Text word = new Text();
    private FloatWritable value = new FloatWritable();

    public void map(Object key, Text text, Context context) throws IOException, InterruptedException {
        String line = text.toString();
        String[] words = line.split(",");
        if (words.length > 13) {
            String keyText = words[3].trim();
            float valueFloat = Float.parseFloat(words[13]);
            word.set(keyText);
            value.set(valueFloat);
            context.write(word, value);
        }
    }
  }

  public static class AverageReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    private FloatWritable average = new FloatWritable();

    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {
      float sum = 0;
      int count = 0;
      for (FloatWritable val : values) {
        sum += val.get();
        count++;
      }
      float avg = sum / count;
      average.set(avg);
      context.write(key, average);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "average calculator");
    job.setJarByClass(AverageCalculator.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(AverageReducer.class);
    job.setReducerClass(AverageReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

