package sort;

import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ABRSorter {
  public static void main(String[] args) throws Exception {

    Path inputPath = new Path(args[0]);
    Path outputDir = new Path(args[1]);

    // Create configuration
    Configuration conf = new Configuration(true);

    // Create job
    Job job = new Job(conf, "sort by value");
    job.setJarByClass(ABRSorter.class);

    // Setup MapReduce
    job.setMapperClass(ABRSorter.MapTask.class);
    job.setReducerClass(ABRSorter.ReduceTask.class);
    job.setNumReduceTasks(1);

    // Specify key / value
    job.setMapOutputKeyClass(DoubleWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(DoubleWritable.class);
    job.setOutputValueClass(Text.class);
    job.setSortComparatorClass(IntComparator.class);
    // Input
    FileInputFormat.addInputPath(job, inputPath);
    job.setInputFormatClass(TextInputFormat.class);

    // Output
    FileOutputFormat.setOutputPath(job, outputDir);
    job.setOutputFormatClass(TextOutputFormat.class);

    // Execute job
    int code = job.waitForCompletion(true) ? 0 : 1;
    System.exit(code);

  }

  public static class IntComparator extends WritableComparator {

    public IntComparator() {
      super(DoubleWritable.class);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1,
        byte[] b2, int s2, int l2) {

      Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
      Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();

      return v1.compareTo(v2) * (-1);
    }
  }
  public static class MapTask extends
      Mapper<LongWritable, Text, DoubleWritable, Text> {
    public void map(LongWritable key, Text value, Context context)
        throws java.io.IOException, InterruptedException {
      String line = value.toString();
      String[] tokens = line.split("\t"); // This is the delimiter between Key and Value
      double valuePart = Double.parseDouble(tokens[1]);
      context.write(new DoubleWritable(valuePart), new Text(tokens[0]));
    }
  }

  public static class ReduceTask extends
      Reducer<DoubleWritable, Text, Text, DoubleWritable> {
    public void reduce(DoubleWritable key, Iterable<Text> list, Context context)
        throws java.io.IOException, InterruptedException {

      for (Text value : list) {
        context.write(value,key);
      }

    }
  }
}                                       