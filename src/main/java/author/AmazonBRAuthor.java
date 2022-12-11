package author;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AmazonBRAuthor {

  public static class TokenizerMapper
      extends Mapper<Object, Text, Text, IntWritable>{

    private final CSVParser csvParser = new CSVParserBuilder().withSeparator(',').withQuoteChar('"').build();

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {

      String[] line = this.csvParser.parseLine(value.toString());
//      AuthorScorePair mapKey = new AuthorScorePair();

      Text mapKey = new Text();

      IntWritable mapValue = new IntWritable();

      if (line.length > 0 && checkEntry(line)) {

        mapValue.set(Integer.parseInt(line[16]));
//        mapKey.setAuthorName(line[3]);
//        mapKey.setScore(Integer.parseInt(line[16]));
        mapKey.set(line[3]);

        context.write(mapKey,mapValue);

      }
    }


    public boolean checkEntry(String[] row){

//      if row is empty or if a param is missing return false
      if (row.length == 0){
        return false;
      }
//            if ID/ Title / Author / Rating is missing return false
      if (row[0].isEmpty() || row[1].isEmpty() || row[3].isEmpty() || row[16].isEmpty()) {
        return false;
      }
      return true;
    }

  }

//  public static class CustomPartitioner extends Partitioner<AuthorScorePair , IntWritable> {
//    public int getPartition(AuthorScorePair pair, IntWritable text, int numReduceTasks) {
//      // make sure that partitions are non-negative
//      return Math.abs(pair.getAuthorName().hashCode() % numReduceTasks);
//    }
//  }


//  public static class AuthorScoreGroupingComparator extends WritableComparator {
//
//    protected AuthorScoreGroupingComparator() {
//      super(AuthorScorePair.class, true);
//    }
//
//    @Override
//    public int compare(WritableComparable a1, WritableComparable a2) {
//      AuthorScorePair air1 = (AuthorScorePair) a1;
//      AuthorScorePair air2 = (AuthorScorePair) a2;
//      return air1.getAuthorName().compareTo(air2.getAuthorName());
//    }
//  }

  public static class IntSumReducer
      extends Reducer<Text,IntWritable,Text, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
        Context context
    ) throws IOException, InterruptedException {

      int score = 0;
      int total_count = 0;
      for (IntWritable value: values
      ) {
//        Integer entry = Integer.parseInt(value.toString());
        score += value.get();
        total_count += 1;
      }
//      result.set((double)score/total_count);
      result.set((double)score/total_count);

      context.write(key, result);

    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "average rating");
    job.setJarByClass(AmazonBRAuthor.class);
    //Set number of reducer tasks
//        job.setNumReduceTasks(10);
    job.setMapperClass(AmazonBRAuthor.TokenizerMapper.class);
    job.setReducerClass(AmazonBRAuthor.IntSumReducer.class);
//    job.setPartitionerClass(CustomPartitioner.class);
//    job.setGroupingComparatorClass(AuthorScoreGroupingComparator.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}