package mpproject;

import org.apache.hadoop.conf.Configuration;

public class Main {
  public static void main(String[] args) {
    assert args.length == 1;

    Configuration conf = Utils.loadConfig(args[0]);

    try {
      DomainWordCountController.run(conf);
      CoOccurController.run(conf);
      AllWordCountController.run(conf);
      PmiController.run(conf);
    } catch (Exception e) {
      throw new Error("Mapreduce error", e);
    }
  }
}
