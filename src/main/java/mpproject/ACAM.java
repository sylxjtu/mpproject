package mpproject;

import com.hankcs.algorithm.AhoCorasickDoubleArrayTrie;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @class AC自动机类，用于领域词表与语料库文章的匹配
 * 提供AC自动机的创建，并提供把AC自动机保存和加载的方法
 */
public class ACAM {
  private AhoCorasickDoubleArrayTrie<String> acdat;
  private Configuration conf;

  public ACAM(Configuration conf) {
    this.conf = conf;
  }

  /**
   * @param args
   * @throws IOException 加载领域词表，进行AC自动机的初始化
   */

  public static void main(String[] args) throws IOException {

    assert args.length == 1;

    Configuration conf = Utils.loadConfig(args[0]);

    Path getFilePath = new Path(conf.get(Utils.WORD_LIST_PATH_KEY));
    FileSystem fs = FileSystem.get(conf);
    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));
    String line = null;
    Map<String, String> map = new HashMap<>();
    while ((line = reader.readLine()) != null) {
      String word = line.trim();
      map.put(word, word);
    }

    // 创建AC自动机
    ACAM acam = new ACAM(conf);

    // 初始化AC自动机
    acam.loadWords(map);

    // 保存AC自动机
    acam.save(conf.get(Utils.ACAM_PATH_KEY));
  }

  /**
   * @param words 领域词表
   * @return 构建成功的AC自动机
   */

  public void loadWords(Map<String, String> words) {
    acdat = new AhoCorasickDoubleArrayTrie<>();
    acdat.build(words);
  }


  /**
   * @param path HDFS上保存初始化后AC自动机的路径
   * @throws IOException
   */
  public void save(String path) throws IOException {
    Path savePath = new Path(path);
    FileSystem fs = FileSystem.get(conf);
    FSDataOutputStream stream = fs.create(savePath);
    acdat.save(new ObjectOutputStream(stream.getWrappedStream()));
    stream.flush();
    stream.close();
  }

  /**
   * @param path 加载AC自动机的路径
   * @throws IOException
   */

  public void load(String path) throws IOException {
    acdat = new AhoCorasickDoubleArrayTrie<>();
    FSDataInputStream stream = null;
    try {
      Path savePath = new Path(path);
      FileSystem fs = FileSystem.get(conf);
      stream = fs.open(savePath);
      acdat.load(new ObjectInputStream(stream.getWrappedStream()));
      stream.close();
    } catch (ClassNotFoundException e) {
      throw new Error("ACAM load fail", e);
    } finally {
      if (stream != null) {
        stream.close();
      }
    }
  }

  /**
   * @param text
   * @return 匹配成功的结果对象
   */
  public List<AhoCorasickDoubleArrayTrie.Hit<String>> parseText(CharSequence text) {
    return acdat.parseText(text);
  }
}
