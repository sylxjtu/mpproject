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

public class ACAM {
  private AhoCorasickDoubleArrayTrie<String> acdat;
  private Configuration conf;

  public ACAM(Configuration conf) {
    this.conf = conf;
  }

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

    ACAM acam = new ACAM(conf);
    acam.loadWords(map);
    acam.save(conf.get(Utils.ACAM_PATH_KEY));
  }

  public void loadWords(Map<String, String> words) {
    acdat = new AhoCorasickDoubleArrayTrie<>();
    acdat.build(words);
  }

  public void save(String path) throws IOException {
    Path savePath = new Path(path);
    FileSystem fs = FileSystem.get(conf);
    FSDataOutputStream stream = fs.create(savePath);
    acdat.save(new ObjectOutputStream(stream.getWrappedStream()));
    stream.flush();
    stream.close();
  }

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

  public List<AhoCorasickDoubleArrayTrie.Hit<String>> parseText(CharSequence text) {
    return acdat.parseText(text);
  }
}
