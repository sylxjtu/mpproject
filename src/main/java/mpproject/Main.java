package mpproject;

import org.apache.hadoop.conf.Configuration;

public class Main {
  public static void main(String[] args) {
    assert args.length == 6;

    Configuration conf = new Configuration();
    conf.set(Utils.CORPUS_PATH_KEY, args[0]);
    conf.set(Utils.WORD_LIST_PATH_KEY, args[1]);
    conf.set(Utils.WORD_COUNT_PATH_KEY, args[2]);
    conf.set(Utils.WORD_CO_OCCUR_PATH_KEY, args[3]);
    conf.set(Utils.WORD_ALL_COUNT_PATH_KEY, args[4]);
    conf.set(Utils.PMI_PATH_KEY, args[5]);

    try {
      DomainWordCountController.run(conf);
      CoOccurController.run(conf);
      AllWordCountController.run(conf);
      PmiController.run(conf);
    } catch (Exception e) {
      throw new Error("", e);
    }
  }
}
