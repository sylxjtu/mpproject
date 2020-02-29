package mpproject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @function 统计语料库中所出现的总词数
 */
public class AllWordCountController {
  public static void run(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {

    // 从configuration对象中获取分词后语料库输入路径
    // 从configuration对象中获取预料库词数统计结果输出路径
    Path inputPath = new Path(conf.get(Utils.CORPUS_PATH_KEY));
    Path outputPath = new Path(conf.get(Utils.WORD_ALL_COUNT_PATH_KEY));
    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true);
    }

    Job job = Job.getInstance(conf, "MPProject All Word Count");

    // 设置该MapReduce作业运行的相关参数，
    // 如输入输出路径、Mapper类、Combiner类、Reducer类等
    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);
    job.setJarByClass(AllWordCountController.class);
    job.setMapperClass(AllWordCountMapper.class);
    job.setCombinerClass(SumReducer.class);
    job.setReducerClass(SumReducer.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(LongWritable.class);

    if(!job.waitForCompletion(true)) {
      throw new Error("job failed");
    }
  }

  private AllWordCountController() {}
}
