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

/**
 * 统计领域词在语料库中出现频率的MapReduce任务
 */
public class DomainWordCountController {
  public static void run(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {

    // 从configuration对象中获取原始语料库输入路径
    // 从configuration对象中获取领域词统计结果输出路径
    Path inputPath = new Path(conf.get(Utils.RAW_CORPUS_PATH_KEY));
    Path outputPath = new Path(conf.get(Utils.WORD_COUNT_PATH_KEY));
    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true);
    }

    Job job = Job.getInstance(conf, "MPProject Word Count");

    // 从configuration对象中获取AC自动机（用于字符串匹配）存储的路径
    String cacheFile = conf.get(Utils.ACAM_PATH_KEY);

    // 并把该文件传送到DistributedCache中
    job.addCacheFile(URI.create(cacheFile));

    // 设置该MapReduce作业运行的相关参数，
    // 如输入输出路径、Mapper类、Combiner类、Reducer类等
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
