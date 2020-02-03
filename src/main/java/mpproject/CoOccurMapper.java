package mpproject;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

public class CoOccurMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
  private Set<String> domainWords;
  private static final LongWritable ONE = new LongWritable(1);
  private static final int WINDOW = 10;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    domainWords = new HashSet<>();

    URI[] cacheFiles = context.getCacheFiles();

    assert cacheFiles != null && cacheFiles.length == 1;

    FileSystem fs = FileSystem.get(context.getConfiguration());
    Path getFilePath = new Path(cacheFiles[0].toString());
    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));
    String line = null;
    while ((line = reader.readLine()) != null) {
      String word = line.trim();
      domainWords.add(word);
    }
  }

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    List<String> queue = new LinkedList<>();

    String line = value.toString();
    StringTokenizer tokenizer = new StringTokenizer(line);
    Text output = new Text();
    while (tokenizer.hasMoreTokens()) {
      String word = tokenizer.nextToken();
      queue.add(word);
      if (queue.size() > WINDOW) {
        queue.remove(0);
      }
      if (domainWords.contains(word)) {
        for (String other : queue) {
          if (domainWords.contains(other)) {
            int comp = word.compareTo(other);
            if (comp < 0) {
              output.set(word + Utils.SEPARATOR + other);
              context.write(output, ONE);
            } else if (comp > 0) {
              output.set(other + Utils.SEPARATOR + word);
              context.write(output, ONE);
            }
          }
        }
        output.set(word);
      }
    }
  }
}
