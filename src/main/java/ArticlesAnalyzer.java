import edu.stanford.nlp.ling.Word;
import edu.stanford.nlp.simple.Document;
import edu.stanford.nlp.simple.Sentence;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Int;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
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
        articlesSchema = new StructType(new StructField[]{DataTypes.createStructField("articleName", DataTypes.StringType, true), DataTypes.createStructField("articleContent", DataTypes.StringType, true)});
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
        Document document;
        List<String> tokens = new ArrayList<>();
        List<List<String>> posTags = new ArrayList<>();
        List<String> lemmas = new ArrayList<>();
        Map<String, Integer> frequencies = new HashMap<>();

        for (Integer key : levelArticlesMap.keySet())
        {
            for (String articleName : levelArticlesMap.get(key))
            {
                articleText = dataset
                        .select("articleContent")
                        .where("articleName = '" + articleName + "'")
                        .map((MapFunction<Row, String>) entry -> entry.mkString(), Encoders.STRING())
                        .first();

                document = new Document(articleText);
                for(Sentence sentence : document.sentences())
                {
                    // pos tagging
                    posTags.add(sentence.posTags());
                }

                for(List<String> sentence : posTags)
                {
                    for(String posTag : sentence)
                    {
                        // filter nouns
                        if(posTag.charAt(0) == 'N')
                        {
                            int sentenceIndex = posTags.indexOf(sentence);
                            int wordIndex = sentence.indexOf(posTag);

                            // lemmatization
                            lemmas.add(document.sentence(sentenceIndex).lemma(wordIndex));
                        }
                    }
                }
            }
        }
        // create map from list
        for (String lemma : lemmas)
        {
            if (frequencies.containsKey(lemma))
            {
                frequencies.put(lemma, frequencies.get(lemma) + 1);
            }
            else frequencies.put(lemma, 1);
        }

        // create csv from map
        CsvWriter csvWriter = new CsvWriter();
        csvWriter.createCsvFromMap(frequencies, CSV_DIRECTORY + "noun_frequencies.csv");

        // visualize distribution in python (word clouds)
        // TODO OPTION: count noun only once per article

    }

    // NOTES
    // use MLlib whenever possible





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
