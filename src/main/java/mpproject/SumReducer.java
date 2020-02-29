package mpproject;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @function 对map阶段发送的键值对进行reduce操作
 */
public class SumReducer extends Reducer<Object, LongWritable, Object, LongWritable> {
  @Override
  protected void reduce(Object key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
    long output = 0;

    // 对map阶段发送的键相同，即单词相同的值进行叠加，得到每个词的统计结果
    for (LongWritable x : values) {
      output += x.get();
    }

    // 用相同键发送，得到最终统计的总数
    context.write(key, new LongWritable(output));
  }
}
