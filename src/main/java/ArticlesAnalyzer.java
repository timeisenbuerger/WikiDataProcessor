import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
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
      sparkConf = new SparkConf().setAppName("analyzeArticles").setMaster("local[*]");
      sparkContext = new SparkContext(sparkConf);
      sqlContext = new SQLContext(new SparkSession(sparkContext));
   }

   public void merge() throws IOException
   {
      FileSystem fileSystem = FileSystem.get(sparkContext.hadoopConfiguration());
      FileUtil.copyMerge(fileSystem, new Path(ARTICLE_TEXT_DIRECTORY + "article_text_csvs"), fileSystem,
            new Path(ARTICLE_TEXT_DIRECTORY + "article_texts.csv"), true, sparkContext.hadoopConfiguration(), null
      );
   }

   public void analyzeArticleTexts() throws FileNotFoundException
   {
      File levelArticleTitles = new File(CSV_DIRECTORY + "level_article_title.csv");
      if( levelArticleTitles.exists() )
      {
         BufferedReader bufferedReader = new BufferedReader(new FileReader(levelArticleTitles));
         List<String> lines = bufferedReader.lines().collect(Collectors.toList());

         Dataset<Row> articleTexts = sqlContext.read().schema(articlesSchema)
               .option("delimiter", ";")
               .csv(ARTICLE_TEXT_DIRECTORY + "article_texts.csv")
               .cache();

         for( String line : lines )
         {
            String level = line.split(";")[0];
            String titles = line.split(";")[1];

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
                  possibleResult.show();
                  System.out.println(possibleResult.first());
               }
            }
         }
      }
   }

   //private void countNouns(Map<Integer, List<String>> levelArticlesMap, boolean oncePerArticle)
   //{
   //   String articleText;
   //   Document document;
   //   List<List<String>> partOfSpeechTags = new ArrayList<>();
   //   List<String> lemmas = new ArrayList<>();
   //   Map<String, Integer> frequencies = new HashMap<>();
   //
   //   for( Integer key : levelArticlesMap.keySet() )
   //   {
   //      for( String articleName : levelArticlesMap.get(key) )
   //      {
   //         articleText = dataset
   //               .select("articleContent")
   //               .where("articleName = '" + articleName + "'")
   //               .map((MapFunction<Row, String>) entry -> entry.mkString(), Encoders.STRING())
   //               .first();
   //
   //         document = new Document(articleText);
   //         for( Sentence sentence : document.sentences() )
   //         {
   //            // pos tagging
   //            partOfSpeechTags.add(sentence.posTags());
   //         }
   //
   //         for( List<String> sentence : partOfSpeechTags )
   //         {
   //            for( int i = 0; i < sentence.size(); i++ )
   //            {
   //               String posTag = sentence.get(i);
   //
   //               // filter nouns
   //               if( posTag.startsWith("N") )
   //               {
   //                  int sentenceIndex = partOfSpeechTags.indexOf(sentence);
   //
   //                  // lemmatization
   //                  lemmas.add(document.sentence(sentenceIndex).lemma(i));
   //               }
   //            }
   //         }
   //      }
   //   }
   //   // create map from list
   //   for( String lemma : lemmas )
   //   {
   //      if( frequencies.containsKey(lemma) )
   //      {
   //         frequencies.put(lemma, frequencies.get(lemma) + 1);
   //      }
   //      else
   //      {
   //         frequencies.put(lemma, 1);
   //      }
   //   }
   //
   //   // create csv from map
   //   CsvWriter csvWriter = new CsvWriter();
   //   csvWriter.createCsvFromMap(frequencies, CSV_DIRECTORY + "noun_frequencies.csv");
   //
   //   // visualize distribution in python (word clouds)
   //   // TODO OPTION: count noun only once per article
   //
   //}
   //
   //// NOTES
   //// use MLlib whenever possible
   //
   //// TOPICS
   //// filter stop words from lemmas
   //// prepare data (bow representation, dictionary...)
   //// LDA
   //// visualize clusters
   //// cluster evaluation (mutual information/purity, silhouette, ...)
   //// perform multiple times with varying number of levels (cumulative)
   //// compare number of clusters and metrics
   //
   //private void countNamedEntities(Map<Integer, List<String>> levelArticlesMap)
   //{
   //   String articleText;
   //   Document document;
   //   List<String> namedEntities = new ArrayList<>();
   //   Map<String, Integer> frequencies = new HashMap<>();
   //
   //   // TODO avoid duplicate code
   //   for( Integer key : levelArticlesMap.keySet() )
   //   {
   //      for( String articleName : levelArticlesMap.get(key) )
   //      {
   //         articleText = dataset
   //               .select("articleContent")
   //               .where("articleName = '" + articleName + "'")
   //               .map((MapFunction<Row, String>) entry -> entry.mkString(), Encoders.STRING())
   //               .first();
   //
   //         document = new Document(articleText);
   //         for( Sentence sentence : document.sentences() )
   //         {
   //            // ner recognition
   //            for( int i = 0; i < sentence.nerTags().size(); i++ )
   //            {
   //               String ner = sentence.nerTag(i);
   //               if( !ner.equals("O") )
   //               {
   //                  namedEntities.add(sentence.word(i));
   //               }
   //            }
   //         }
   //      }
   //
   //      // TODO avoid duplicate code
   //      // create map from list
   //      for( String namedEntity : namedEntities )
   //      {
   //         if( frequencies.containsKey(namedEntity) )
   //         {
   //            frequencies.put(namedEntity, frequencies.get(namedEntity) + 1);
   //         }
   //         else
   //         {
   //            frequencies.put(namedEntity, 1);
   //         }
   //      }
   //
   //      // create csv from map
   //      CsvWriter csvWriter = new CsvWriter();
   //      csvWriter.createCsvFromMap(frequencies, CSV_DIRECTORY + "named_entity_frequencies.csv");
   //   }
   //}
}
