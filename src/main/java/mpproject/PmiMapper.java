package mpproject;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;


/**
 * @class PMI计算Mapper类
 */
public class PmiMapper extends Mapper<Text, Text, Text, DoubleWritable> {
  Map<String, Long> wordOccurCount;
  long wordCount;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    wordOccurCount = new HashMap<>();

    URI[] cacheFiles = context.getCacheFiles();

    assert cacheFiles != null && cacheFiles.length == 2;

    // 从领域单词频率统计的结果文件中读取每个单词的频率
    FileSystem fs = FileSystem.get(context.getConfiguration());
    Path getFilePath = new Path(cacheFiles[0].toString());
    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));
    String line = null;
    while ((line = reader.readLine()) != null) {
      String[] kv = line.split("\t");
      if (kv.length != 2) {
        System.err.println("Bad kv: " + kv);
        System.exit(1);
      }
      String word = kv[0];
      long count = Long.parseLong(kv[1]);

      // 领域单词统计频率装在map中，以供查询
      wordOccurCount.put(word, count);
    }

    // 读取语料库单词总数统计结果
    getFilePath = new Path(cacheFiles[1].toString());
    reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));
    wordCount = Long.parseLong(reader.readLine());
  }

  /**
   *  输入：共现单词对的频率
   *  输出：共现单词对的PMI
   */
  @Override
  protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
    String[] wordPair = key.toString().split(Utils.SEPARATOR);

    // 共现单词对前一个单词和后一个单词
    String firstWord = wordPair[0];
    String secondWord = wordPair[1];

    // 根据PMI计算公式计算每一个单词对的PMI
    double lpxy = Utils.log2(Long.parseLong(value.toString())) - Utils.log2(wordCount);
    double lpx = Utils.log2(wordOccurCount.get(firstWord)) - Utils.log2(wordCount);
    double lpy = Utils.log2(wordOccurCount.get(secondWord)) - Utils.log2(wordCount);
    double pmi = lpxy - lpx - lpy;

    context.write(key, new DoubleWritable(pmi));
  }
}
