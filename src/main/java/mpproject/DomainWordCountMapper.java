package mpproject;

import com.hankcs.algorithm.AhoCorasickDoubleArrayTrie;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * @function 统计领域词出行频率的Mapper
 */
public class DomainWordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

  private ACAM acam;
  private static final LongWritable ONE = new LongWritable(1);

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    // 获取存储在分布式缓存中的AC自动机路径
    URI[] cacheFiles = context.getCacheFiles();

    assert cacheFiles != null && cacheFiles.length == 1;

    // 创建AC自动机对象
    acam = new ACAM(context.getConfiguration());

    // 加载利用领域词进行初始化的AC自动机
    acam.load(cacheFiles[0].toString());
  }

  @Override
  protected void map(LongWritable key, Text value, Context context)
          throws IOException, InterruptedException {
    String line = value.toString();
    Text output = new Text();

    // 把输入到Mapper中的语料库行作为查询字符串，与初始化的AC自动机进行匹配
    // 返回查询字符串中与AC自动机中的模式串匹配的matches
    // 每个match对象中包含匹配的开始和结束位置，以及匹配的字符串的值
    // 返回的匹配对象类型是AhoCorasickDoubleArrayTrie.Hit<String>
    List<AhoCorasickDoubleArrayTrie.Hit<String>> matches = acam.parseText(line);
    for (AhoCorasickDoubleArrayTrie.Hit<String> match : matches) {

      output.set(match.value);
      // 对于每个匹配的字符串，发送<string, 1>键值对
      context.write(output, ONE);
    }
  }
}
