import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class ArticlesAnalyzer implements Serializable
{
   private static SparkConf sparkConf;
   private static SparkContext sparkContext;
   private static SQLContext sqlContext;
   private static JavaSparkContext jsc;

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
      File levelArticleTitles = new File(PathConstants.CSV_DIRECTORY + "level_article_title.csv");
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


         Map<String, Integer> nounsFrequencies = new HashMap<>();
         Map<String, Integer> nameEntityFrequencies = new HashMap<>();

         //broadcast through partitions so values can be written
         Broadcast<Map<String, Integer>> broadcastNounsFrequencies = jsc.broadcast(nounsFrequencies);
         Broadcast<Map<String, Integer>> broadcastNamedFrequencies = jsc.broadcast(nameEntityFrequencies);

         sqlContext.read().schema(SchemaConstants.articlesSchema)
               .option("delimiter", ";")
               .csv(PathConstants.ARTICLE_TEXT_DIRECTORY + "article_texts.csv")
               .filter((Row x) -> titles.contains(x.get(0)))
               .dropDuplicates()
               .foreach((Row x) -> {
                  countNouns((String) x.get(1), broadcastNounsFrequencies.getValue(), false);
                  countNamedEntities((String) x.get(1), broadcastNamedFrequencies.getValue());
               });

         sqlContext.read().csv(PathConstants.CSV_DIRECTORY + "level_article_title.csv");

         System.out.println("Jetzt schreibe ich");
         System.out.println("Nouns +" + broadcastNounsFrequencies.getValue().size());
         System.out.println("NamedEntities +" + broadcastNamedFrequencies.getValue().size());

         writeMapAsCsv(broadcastNounsFrequencies.getValue(), PathConstants.CSV_DIRECTORY + "nouns_frequencies.csv");
         writeMapAsCsv(broadcastNamedFrequencies.getValue(), PathConstants.CSV_DIRECTORY + "named_entity_frequencies.csv");

         sqlContext.read().schema(SchemaConstants.articlesSchema)
               .option("delimiter", ";")
               .csv(PathConstants.ARTICLE_TEXT_DIRECTORY + "article_texts.csv")
               .filter((Row x) -> titles.contains(x.get(0)))
               .dropDuplicates().write().option("delimiter", ";").csv(PathConstants.CSV_DIRECTORY + "neededArticleTitlesAndContents.csv");

         FileSystem fileSystem = FileSystem.get(sparkContext.hadoopConfiguration());
         FileUtil.copyMerge(fileSystem, new Path(PathConstants.CSV_DIRECTORY + "neededArticleTitlesAndContents.csv"), fileSystem,
               new Path(PathConstants.CSV_DIRECTORY + "needed_article_titles_and_contents.csv"), true, sparkContext.hadoopConfiguration(), null
         );
      }
   }

   private List<String> filterLemmas(String articleContent) throws FileNotFoundException
   {
      Document document = new Document(articleContent);
      List<String> allLemmas = new ArrayList<>();
      for( Sentence sentence : document.sentences() )
      {
         allLemmas.addAll(sentence.lemmas());
      }

      BufferedReader bufferedReader = new BufferedReader(new FileReader(new File("E:\\Entwicklung\\WikiDataProcessor\\lib\\stopwords.txt")));
      List<String> stopwordList = bufferedReader.lines().collect(Collectors.toList());

      return allLemmas.stream().filter(lemma -> lemma.length() > 3 && !stopwordList.contains(lemma)).collect(Collectors.toList());
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
