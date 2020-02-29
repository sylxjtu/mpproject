package mpproject;

import com.hankcs.algorithm.AhoCorasickDoubleArrayTrie;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * @class 进行单词共现频率的统计的Mapper类
 */
public class CoOccurMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

  // 设置单词共现的窗口大小
  private static final int WINDOW = 20;
  private static final LongWritable ONE = new LongWritable(1);
  private ACAM acam;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    URI[] cacheFiles = context.getCacheFiles();

    assert cacheFiles != null && cacheFiles.length == 1;

    acam = new ACAM(context.getConfiguration());

    // 加载初始化的AC自动机
    acam.load(cacheFiles[0].toString());
  }

  /**
   *    输入<long, text>
   *    输出<word1###word2, long>
   */
  @Override
  protected void map(LongWritable key, Text value, Context context)
          throws IOException, InterruptedException {
    String line = value.toString();

    List<AhoCorasickDoubleArrayTrie.Hit<String>> matches = acam.parseText(line);
    Text output = new Text();

    for (AhoCorasickDoubleArrayTrie.Hit<String> match : matches) {
      for (AhoCorasickDoubleArrayTrie.Hit<String> otherMatch : matches) {

        // Only consider one direction
        // 仅仅考虑领域词表中一个顺序的单词对
        // 如word1在word2前面，仅仅考虑word1###word2,不考虑word2###word1
        if (match.value.compareTo(otherMatch.value) >= 0) continue;

        // Overlap, ignore
        // 如果两个匹配单词有重叠部分则忽略
        if (match.begin >= otherMatch.begin && match.begin < otherMatch.end) continue;
        if (otherMatch.begin >= match.begin && otherMatch.begin < match.end) continue;

        // Non-overlap, check window
        // 如果单词没有重叠部分，且两个单词的距离在window的大小之内，则发送该单词对的键值对
        if ((match.end <= otherMatch.begin && otherMatch.begin - match.end <= WINDOW)
                || (otherMatch.end <= match.begin && match.begin - otherMatch.end <= WINDOW)) {
          output.set(match.value + Utils.SEPARATOR + otherMatch.value);
          context.write(output, ONE);
        }
      }
    }
  }
}
