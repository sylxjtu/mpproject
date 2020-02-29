package mpproject;

import org.apache.hadoop.conf.Configuration;


/**
 * 项目控制的主类程序
 * 项目运行的入口
 */
public class Main {
  public static void main(String[] args) {
    assert args.length == 1;

    // 加载项目运行所需的全局参数，并保存在configuration对象中
    Configuration conf = Utils.loadConfig(args[0]);

    try {


      // 统计领域词在语料库中出现的频率
      DomainWordCountController.run(conf);

      // 统计领域词在语料库中共现的频率
      CoOccurController.run(conf);

      // 语料库中多所有单词的总数
      AllWordCountController.run(conf);

      //pmi的计算
      PmiController.run(conf);

    } catch (Exception e) {
      throw new Error("Mapreduce error", e);
    }
  }
}
