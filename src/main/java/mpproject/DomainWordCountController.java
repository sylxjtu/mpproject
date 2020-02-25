package mpproject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

public class DomainWordCountController {
  public static void run(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {
    Path inputPath = new Path(conf.get(Utils.RAW_CORPUS_PATH_KEY));
    Path outputPath = new Path(conf.get(Utils.WORD_COUNT_PATH_KEY));
    FileSystem fs = FileSystem.get(conf);
    if(fs.exists(outputPath)) {
      fs.delete(outputPath, true);
    }

    Job job = Job.getInstance(conf, "MPProject Word Count");

    String cacheFile = conf.get(Utils.ACAM_PATH_KEY);
    job.addCacheFile(URI.create(cacheFile));

    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);
    job.setJarByClass(DomainWordCountController.class);
    job.setMapperClass(DomainWordCountMapper.class);
    job.setCombinerClass(SumReducer.class);
    job.setReducerClass(SumReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    if(!job.waitForCompletion(true)) {
      throw new Error("job failed");
    }
  }

  private DomainWordCountController() {}
}
