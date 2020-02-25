package mpproject;

import com.hankcs.algorithm.AhoCorasickDoubleArrayTrie;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URI;
import java.util.List;

public class CoOccurMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
  private static final int WINDOW = 20;
  private static final LongWritable ONE = new LongWritable(1);
  private ACAM acam;

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
    List<AhoCorasickDoubleArrayTrie.Hit<String>> matches = acam.parseText(line);
    Text output = new Text();
    for (AhoCorasickDoubleArrayTrie.Hit<String> match : matches) {
      for (AhoCorasickDoubleArrayTrie.Hit<String> otherMatch : matches) {
        // Only consider one direction
        if (match.value.compareTo(otherMatch.value) >= 0) continue;
        // Overlap, ignore
        if (match.begin >= otherMatch.begin && match.begin < otherMatch.end) continue;
        if (otherMatch.begin >= match.begin && otherMatch.begin < match.end) continue;
        // Non-overlap, check window
        if ((match.end <= otherMatch.begin && otherMatch.begin - match.end <= WINDOW)
                || (otherMatch.end <= match.begin && match.begin - otherMatch.end <= WINDOW)) {
          output.set(match.value + Utils.SEPARATOR + otherMatch.value);
        }
      }
    }
  }
}
