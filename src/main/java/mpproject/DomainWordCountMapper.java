package mpproject;

import com.hankcs.algorithm.AhoCorasickDoubleArrayTrie;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URI;
import java.util.List;

public class DomainWordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
  private ACAM acam;
  private static final LongWritable ONE = new LongWritable(1);

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    URI[] cacheFiles = context.getCacheFiles();

    assert cacheFiles != null && cacheFiles.length == 1;

    acam = new ACAM(context.getConfiguration());
    acam.load(cacheFiles[0].toString());
  }

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String line = value.toString();
    Text output = new Text();

    List<AhoCorasickDoubleArrayTrie.Hit<String>> matches = acam.parseText(line);
    for (AhoCorasickDoubleArrayTrie.Hit<String> match : matches) {
      output.set(match.value);
      context.write(output, ONE);
    }
  }
}
