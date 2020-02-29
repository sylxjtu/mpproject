package mpproject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

// PMI计算控制主类
public class PmiController {
  public static void run(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {

    // 从configuration对象中获取单词共现统计结果作为输入路径
    // 从configuration对象中获取PMI计算结果输出路径
    Path inputPath = new Path(conf.get(Utils.WORD_CO_OCCUR_PATH_KEY));
    Path outputPath = new Path(conf.get(Utils.PMI_PATH_KEY));
    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true);
    }

    Job job = Job.getInstance(conf, "MPProject Co Occur");

    // 把领域词频率统计结果文件传送到DistributedCache中
    job.addCacheFile(URI.create(conf.get(Utils.WORD_COUNT_PATH_KEY) + "/part-r-00000"));
    // 把语料库总词数统计结果文件传送到DistributedCache中
    job.addCacheFile(URI.create(conf.get(Utils.WORD_ALL_COUNT_PATH_KEY) + "/part-r-00000"));

    // 设置该MapReduce作业运行的相关参数，
    // 如输入输出路径、Mapper类、Combiner类、Reducer类等
    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);
    job.setJarByClass(PmiController.class);
    job.setMapperClass(PmiMapper.class);
    job.setInputFormatClass(KeyValueTextInputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    job.setNumReduceTasks(0);

    if(!job.waitForCompletion(true)) {
      throw new Error("job failed");
    }
  }

  private PmiController() {}
}
