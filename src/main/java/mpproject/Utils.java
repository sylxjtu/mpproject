package mpproject;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.util.Map;


/**
 * @function 定义一些项目中计算所用到的工具
 * 加载项目运行所需的配置参数的方法
 */
public class Utils {

  // 保存项目中所用参数名称 的变量
  public static final String CORPUS_PATH_KEY = "X-CORPUS-PATH";
  public static final String RAW_CORPUS_PATH_KEY = "X-RAW-CORPUS-PATH";
  public static final String WORD_LIST_PATH_KEY = "X-WORD-LIST-PATH";
  public static final String WORD_COUNT_PATH_KEY = "X-WORD-COUNT-PATH";
  public static final String WORD_CO_OCCUR_PATH_KEY = "X-WORD-CO-OCCUR-PATH";
  public static final String WORD_ALL_COUNT_PATH_KEY = "X-WORD-ALL-COUNT-PATH";
  public static final String PMI_PATH_KEY = "X-PMI-PATH";
  public static final String ACAM_PATH_KEY = "X-ACAM-PATH";
  public static final String SEPARATOR = "###";

  private Utils() {
  }

  // 计算PMI所需的数学公式
  public static double log2(double x) {
    return Math.log(x) / Math.log(2);
  }


  // 该函数的功能是加载该项目运行所需的参数配置文件
  // 并设置在Configuration对象中，作为Configuration的属性进行保存和获取
  public static Configuration loadConfig(String path) {

    Configuration conf = new Configuration();
    ObjectMapper mapper = new ObjectMapper();
    try {
      Map<String, String> config = mapper.readValue(FileUtils.readFileToString(new File(path)), Map.class);
      config.forEach(conf::set);
    } catch (Exception e) {
      throw new Error("Cannot open configure file", e);
    }
    return conf;
  }
}
