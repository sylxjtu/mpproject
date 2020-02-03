package mpproject;

public class Utils {
  public static final String CORPUS_PATH_KEY = "X-CORPUS-PATH";
  public static final String WORD_LIST_PATH_KEY = "X-WORD-LIST-PATH";
  public static final String WORD_COUNT_PATH_KEY = "X-WORD-COUNT-PATH";
  public static final String WORD_CO_OCCUR_PATH_KEY = "X-WORD-CO-OCCUR-PATH";
  public static final String WORD_ALL_COUNT_PATH_KEY = "X-WORD-ALL-COUNT-PATH";
  public static final String PMI_PATH_KEY = "X-PMI-PATH";
  public static final String SEPARATOR = "###";

  private Utils() {}

  public static double log2(double x) {
    return Math.log(x) / Math.log(2);
  }
}
