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
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class DomainWordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
  private Set<String> domainWords;
  private static final LongWritable ONE = new LongWritable(1);

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
    String line = value.toString();
    StringTokenizer tokenizer = new StringTokenizer(line);
    Text output = new Text();
    while(tokenizer.hasMoreTokens()) {
      String word = tokenizer.nextToken();
      if(domainWords.contains(word)) {
        output.set(word);
        context.write(output, ONE);
      }
    }
  }
}
