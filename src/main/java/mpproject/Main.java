package mpproject;

import org.apache.hadoop.conf.Configuration;

public class Main {
  public static void main(String[] args) {
    assert args.length == 2;

    Configuration conf = Utils.loadConfig(args[1]);

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
