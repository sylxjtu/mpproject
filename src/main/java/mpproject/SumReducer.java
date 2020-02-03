package mpproject;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SumReducer extends Reducer<Object, LongWritable, Object, LongWritable> {
  @Override
  protected void reduce(Object key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
    long output = 0;
    for(LongWritable x: values) {
      output += x.get();
    }
    context.write(key, new LongWritable(output));
  }
}
