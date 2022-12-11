package publisher;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AmazonBRPublisher {

  public static class TokenizerMapper
      extends Mapper<Object, Text, Text, Text>{

    private final CSVParser csvParser = new CSVParserBuilder().withSeparator(',').withQuoteChar('"').build();

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {

      String[] line = this.csvParser.parseLine(value.toString());
      Text mapKey = new Text();
      Text mapValue = new Text();

      if (line.length > 0 && checkEntry(line)) {

        mapValue.set(line[16]);
        mapKey.set(line[6]);

        context.write(mapKey,mapValue);

      }
    }


    public boolean checkEntry(String[] row){

//      if row is empty or if a param is missing return false
      if (row.length == 0){
        return false;
      }
//            if ID/ Title / Category / Rating is missing return false
      if (row[0].isEmpty() || row[1].isEmpty() || row[6].isEmpty() || row[16].isEmpty()) {
        return false;
      }
      return true;
    }

  }

  public static class IntSumReducer
      extends Reducer<Text,Text,Text,Text> {

    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
        Context context
    ) throws IOException, InterruptedException {

      int score = 0;
      int total_count = 0;
      for (Text value: values
      ) {
        Integer entry = Integer.parseInt(value.toString());
        score += entry;
        total_count += 1;
      }
      result.set(String.valueOf((double)score/total_count));
      context.write(key, result);

    }

  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "average rating");
    job.setJarByClass(AmazonBRPublisher.class);
    //Set number of reducer tasks
//        job.setNumReduceTasks(10);
    job.setMapperClass(AmazonBRPublisher.TokenizerMapper.class);
    job.setReducerClass(AmazonBRPublisher.IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}