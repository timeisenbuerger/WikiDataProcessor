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
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ArticlesAnalyzer implements Serializable
{
   private static final String CSV_DIRECTORY = "D:\\Entwicklung\\PTT\\csv\\";
   private static final String ARTICLE_TEXT_DIRECTORY = "D:\\Entwicklung\\PTT\\extracted_text\\";
   //private static final String TXT_DIRECTORY = "D:\\Uni\\05-ws1819\\PTT\\wikidata\\extracted_text";
   //private static final String CSV_DIRECTORY = "D:\\Uni\\05-ws1819\\PTT\\wikidata\\csv";
   private static final String TEMP_DIRECTORY = CSV_DIRECTORY + "\\temp";

   private static SparkConf sparkConf;
   private static SparkContext sparkContext;
   private static SQLContext sqlContext;
   private static JavaSparkContext jsc;

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

   Map<String, Integer> nounsFrequencies = new HashMap<>();
   Map<String, Integer> nameEntityFrequencies = new HashMap<>();

   public ArticlesAnalyzer()
   {
      init();
   }

   private void init()
   {
      sparkConf = new SparkConf().setAppName("analyzeArticles").setMaster("local[*]");
      sparkContext = new SparkContext(sparkConf);
      sqlContext = new SQLContext(new SparkSession(sparkContext));
      jsc = new JavaSparkContext(sparkContext);
   }

   public void analyzeArticleTexts() throws IOException
   {
      File levelArticleTitles = new File(CSV_DIRECTORY + "level_article_title.csv");
      if( levelArticleTitles.exists() )
      {
         BufferedReader bufferedReader = new BufferedReader(new FileReader(levelArticleTitles));
         List<String> lines = bufferedReader.lines().collect(Collectors.toList());
         bufferedReader.close();

         List<String> titles = new ArrayList<>();
         for( String line : lines )
         {
            String title = line.split(";")[1].replace("_", " ");
            titles.addAll(Arrays.asList(title.split(",")));
         }

         Broadcast<Map<String, Integer>> broadcastNounsFrequencies = jsc.broadcast(nounsFrequencies);
         Broadcast<Map<String, Integer>> broadcastNamedFrequencies = jsc.broadcast(nameEntityFrequencies);

         sqlContext.read().schema(articlesSchema)
               .option("delimiter", ";")
               .csv(ARTICLE_TEXT_DIRECTORY + "article_texts.csv")
               .filter((Row x) -> titles.contains(x.get(0)))
               .dropDuplicates()
               .foreach((Row x) -> {
                  countNouns((String) x.get(1), broadcastNounsFrequencies.getValue(), false);
                  countNamedEntities((String) x.get(1), broadcastNamedFrequencies.getValue());
               });

         sqlContext.read().csv(CSV_DIRECTORY + "level_article_title.csv");

         System.out.println("Jetzt schreibe ich");
         System.out.println("Nouns +" + broadcastNounsFrequencies.getValue().size());
         System.out.println("NamedEntities +" + broadcastNamedFrequencies.getValue().size());

         writeMapAsCsv(broadcastNounsFrequencies.getValue(), CSV_DIRECTORY + "nouns_frequencies.csv");
         writeMapAsCsv(broadcastNamedFrequencies.getValue(), CSV_DIRECTORY + "named_entity_frequencies.csv");
      }
   }

   private void countNouns(String articleContent, Map<String, Integer> frequencies, boolean oncePerArticle)
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

   private void countNamedEntities(String articleContent, Map<String, Integer> frequencies)
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
      bufferedWriter.flush();
      bufferedWriter.close();
   }
}
