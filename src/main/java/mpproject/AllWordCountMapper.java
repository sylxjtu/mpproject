package mpproject;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @function 语料库中词数的统计Mapper
 */
public class AllWordCountMapper extends Mapper<LongWritable, Text, NullWritable, LongWritable> {

  /**
   * 输入分词的语料库片段
   * 输出对于每一个词发送一个键值对<NULL, 1>
   */
  @Override
  protected void map(LongWritable key, Text value, Context context)
          throws IOException, InterruptedException {
    String line = value.toString();
    StringTokenizer tokenizer = new StringTokenizer(line);
    LongWritable longWritable = new LongWritable();
    longWritable.set(tokenizer.countTokens());

    // 发送键值对<NULL, 1>
    context.write(NullWritable.get(), longWritable);
  }
}
