import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import edu.stanford.nlp.simple.Document;
import edu.stanford.nlp.simple.Sentence;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ArticlesAnalyzer implements Serializable
{
   private static final String CSV_DIRECTORY = "E:\\Entwicklung\\WikiDumps\\csv\\";
   private static final String ARTICLE_TEXT_DIRECTORY = "E:\\Entwicklung\\WikiDumps\\article_xml\\extracted_text\\";
   //private static final String TXT_DIRECTORY = "D:\\Uni\\05-ws1819\\PTT\\wikidata\\extracted_text";
   //private static final String CSV_DIRECTORY = "D:\\Uni\\05-ws1819\\PTT\\wikidata\\csv";
   private static final String TEMP_DIRECTORY = CSV_DIRECTORY + "\\temp";

   private static SparkConf sparkConf;
   private static SparkContext sparkContext;
   private static SQLContext sqlContext;

   private static StructType levelTitleSchema;
   private static StructType articlesSchema;

   static
   {
      levelTitleSchema = new StructType(new StructField[]{
            DataTypes.createStructField("level", DataTypes.StringType, true),
            DataTypes.createStructField("titles", DataTypes.StringType, true)
      });

      articlesSchema = new StructType(new StructField[]{
            DataTypes.createStructField("articleTitle", DataTypes.StringType, true),
            DataTypes.createStructField("articleContent", DataTypes.StringType, true)
      });
   }

   public ArticlesAnalyzer()
   {
      init();
   }

   private void init()
   {
      sparkConf = new SparkConf().setAppName("analyzeArticles").setMaster("local[4]");
      sparkContext = new SparkContext(sparkConf);
      sqlContext = new SQLContext(new SparkSession(sparkContext));
   }

   public void analyzeArticleTexts() throws IOException
   {
      File levelArticleTitles = new File(CSV_DIRECTORY + "level_article_title.csv");
      if( levelArticleTitles.exists() )
      {
         BufferedReader bufferedReader = new BufferedReader(new FileReader(levelArticleTitles));
         List<String> lines = bufferedReader.lines().collect(Collectors.toList());
         bufferedReader.close();

         Dataset<Row> articleTexts = sqlContext.read().schema(articlesSchema)
               .option("delimiter", ";")
               .csv(ARTICLE_TEXT_DIRECTORY + "article_texts.csv")
               .cache();

         for( String line : lines )
         {
            String level = line.split(";")[0];
            String titles = line.split(";")[1];

            Map<String, Integer> nounsFrequencies = new HashMap<>();
            Map<String, Integer> nameEntityFrequencies = new HashMap<>();

            List<String> titlesAsList = Arrays.asList(titles.split(","));

            for( String title : titlesAsList )
            {
               Dataset<String> possibleResult = articleTexts
                     .select("articleContent")
                     .where("articleTitle = '" + title.replace("_", " ") + "'")
                     .limit(1)
                     .map((MapFunction<Row, String>) entry -> entry.mkString(), Encoders.STRING());

               if( !possibleResult.isEmpty() )
               {
                  String articleContent = possibleResult.first();

                  nounsFrequencies = countNouns(articleContent, nounsFrequencies, false);
                  nameEntityFrequencies = countNamedEntities(articleContent, nameEntityFrequencies);
               }
            }

            if( nounsFrequencies.size() > 0 && nameEntityFrequencies.size() > 0 )
            {
               writeMapAsCsv(nounsFrequencies, CSV_DIRECTORY + "nouns_frequencies" + level.split(" ")[1] + ".csv");
               writeMapAsCsv(nameEntityFrequencies, CSV_DIRECTORY + "named_entity_frequencies" + level.split(" ")[1] + ".csv");
            }
         }
      }
   }

   private Map<String, Integer> countNouns(String articleContent, Map<String, Integer> frequencies, boolean oncePerArticle)
   {
      List<List<String>> partOfSpeechTags = new ArrayList<>();
      List<String> lemmas = new ArrayList<>();

      Document document = new Document(articleContent);
      for( Sentence sentence : document.sentences() )
      {
         // pos tagging
         partOfSpeechTags.add(sentence.posTags());
      }

      for( List<String> sentence : partOfSpeechTags )
      {
         for( int i = 0; i < sentence.size(); i++ )
         {
            String posTag = sentence.get(i);

            // filter nouns
            if( posTag.startsWith("N") )
            {
               int sentenceIndex = partOfSpeechTags.indexOf(sentence);

               // lemmatization
               lemmas.add(document.sentence(sentenceIndex).lemma(i));
            }
         }
      }

      // create map from list
      aggregateMapEntry(frequencies, lemmas);

      return frequencies;

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

   private Map<String, Integer> countNamedEntities(String articleContent, Map<String, Integer> frequencies)
   {
      List<String> namedEntities = new ArrayList<>();

      Document document = new Document(articleContent);
      for( Sentence sentence : document.sentences() )
      {
         // ner recognition
         for( int i = 0; i < sentence.nerTags().size(); i++ )
         {
            String ner = sentence.nerTag(i);
            if( !ner.equals("O") )
            {
               namedEntities.add(sentence.word(i));
            }
         }

         // create map from list
         aggregateMapEntry(frequencies, namedEntities);
      }

      return frequencies;
   }

   private void aggregateMapEntry(Map<String, Integer> frequencies, List<String> elements)
   {
      for( String element : elements )
      {
         if( frequencies.containsKey(element) )
         {
            frequencies.put(element, frequencies.get(element) + 1);
         }
         else
         {
            frequencies.put(element, 1);
         }
      }
   }

   private void writeMapAsCsv(Map<String, Integer> frequencies, String filepath) throws IOException
   {
      File file = new File(filepath);
      if( !file.exists() )
      {
         file.createNewFile();
      }

      BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file));
      for( Map.Entry<String, Integer> entry : frequencies.entrySet() )
      {
         bufferedWriter.write(entry.getKey() + "," + entry.getValue());
         bufferedWriter.newLine();
      }
      bufferedWriter.close();
   }
}
