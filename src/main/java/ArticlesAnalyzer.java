import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Map;

public class ArticlesAnalyzer
{
    private static final String TXT_DIRECTORY = "D:\\Uni\\05-ws1819\\PTT\\wikidata\\extracted_text";
    private static final String CSV_DIRECTORY = "D:\\Uni\\05-ws1819\\PTT\\wikidata\\csv";
    private static final String TEMP_DIRECTORY = CSV_DIRECTORY + "\\temp";

    private static SparkConf sparkConf;
    private static SparkContext sparkContext;
    private static SQLContext sqlContext;

    private static StructType articlesSchema;

    private String articlesPath;
    private Dataset<Row> dataset;

    static
    {
        articlesSchema = new StructType(new StructField[]{
                DataTypes.createStructField("articleName", DataTypes.StringType, true),
                DataTypes.createStructField("articleContent", DataTypes.StringType, true)
        });
    }

    private void init()
    {
        articlesPath = TXT_DIRECTORY + "articles_in_plain_text.txt";

        sparkConf = new SparkConf().setAppName("analyzeArticles").setMaster("local[*]");
        sparkContext = new SparkContext(sparkConf);
        sqlContext = new SQLContext(new SparkSession(sparkContext));

        // TODO update filename
//        dataset = sparkSession.read().csv(CSV_DIRECTORY + "articles_csv.csv");
        dataset = sqlContext.read().schema(articlesSchema).csv(CSV_DIRECTORY + "articles_csv.csv");
    }

    public void countNouns(Map<Integer, List<String>> levelArticlesMap, boolean oncePerArticle)
    {
        String articleText;
        for (Integer key: levelArticlesMap.keySet())
        {
            for (String articleName: levelArticlesMap.get(key))
            {
                articleText = dataset.select("articleContent").where("articleName = '" + articleName + "'").toString();

                // dataSet.map((MapFunction<Row, String>) entry -> entry.mkString(), Encoders.STRING()).collectAsList();

                // tokenization

                // pos tagging
                // filter nouns
                // lemmatization
                // create a map to count occurences
                // add to csv
            }
        }
    }

    // NOTES
    // use MLlib whenever possible

    // NOUNS
    // 1.


    // visualize distribution (word clouds)
    // 2.
    // count noun only once per article

    // TOPICS
    // filter stop words from lemmas
    // prepare data (bow representation, dictionary...)
    // LDA
    // visualize clusters
    // cluster evaluation (mutual information/purity, silhouette, ...)
    // perform multiple times with varying number of levels (cumulative)
    // compare number of clusters and metrics

    // NAMES
    // NER on tokens
    // visualize NER distribution
}
