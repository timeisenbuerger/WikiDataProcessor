import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class DumpProcessor
{
   private SparkConf sparkConf;
   private SparkContext sparkContext;
   private SQLContext sqlContext;

   public DumpProcessor()
   {
      init();
   }

   private void init()
   {
      sparkConf = new SparkConf().setAppName("processData").setMaster("local[*]");
      sparkContext = new SparkContext(sparkConf);
      sqlContext = new SQLContext(new SparkSession(sparkContext));
   }

   public void collectArticlesRelatedToAnimals() throws IOException
   {
      Dataset<Row> joinedCsv = sqlContext.read().schema(SchemaConstants.joinedTableSchema)
            .csv(PathConstants.CSV_DIRECTORY + "page_categorylinks_joined.csv");
      joinedCsv.cache();

      Dataset<Row> animals = joinedCsv.where("cl_to = 'Animals'");

      List<String> resultList = animals.map((MapFunction<Row, String>) entry -> entry.mkString(";"), Encoders.STRING()).collectAsList();

      //Zuordnung von Artikeln zu Ebenen
      Map<Integer, List<String>> levelArticlesMap = new HashMap<>();

      //Wird genutzt, um bereits abgedeckte Subkategorien zu überspringen
      List<String> coveredCategories = new ArrayList<>();
      int level = 0;

      //Hier keine Sparkpartitionen genutzt, da in einer Rekursion dasselbe Dataset (joinedCsv) auf dem wir arbeiten nicht nochmal verwendet werden kann
      //Es entsteht immer eine NullPointerException
      levelArticlesMap = collectLevelArticlesMap(joinedCsv, resultList, levelArticlesMap, coveredCategories, level);

      writeLevelsTitlesFile(levelArticlesMap);
   }

   private void writeLevelsTitlesFile(Map<Integer, List<String>> levelArticlesMap) throws IOException
   {
      File file = new File(PathConstants.CSV_DIRECTORY + "level_article_title.csv");
      BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file));
      CSVPrinter csvPrinter = new CSVPrinter(bufferedWriter, CSVFormat.newFormat(';'));
      for( Map.Entry<Integer, List<String>> entry : levelArticlesMap.entrySet() )
      {
         String titles = "";

         for( int i = 0; i < entry.getValue().size(); i++ )
         {
            String page_title = entry.getValue().get(i);

            if( i != (entry.getValue().size() - 1) )
            {
               page_title += ",";
            }

            titles += page_title;
         }

         csvPrinter.printRecord("Ebene " + entry.getKey(), titles);
         bufferedWriter.newLine();
      }
      csvPrinter.close();
      bufferedWriter.flush();
      bufferedWriter.close();
   }

   private Map<Integer, List<String>> collectLevelArticlesMap(Dataset<Row> joinedCsv, List<String> resultList, Map<Integer, List<String>> levelArticlesMap, List<String> coveredCategories, int level)
   {
      //nur bis level 3, da das sammeln der Daten sonst viel zu lange dauern würde
      if( level < 3 )
      {
         for( String row : resultList )
         {
            List<String> columnValues = Arrays.asList(row.split(";"));
            String page_title = columnValues.get(2);
            String cl_type = columnValues.get(5);

            //Dirty workaround, da es folgende Titel geben kann: Kha'ir. Würde eine SQL-Exception hervorrufen
            if( page_title.contains("'") )
            {
               continue;
            }

            //Es gibt Einträge mit cl_type = null. Hier kann man nicht definieren, ob es eine page oder subcat ist; wir gehen von einer page aus.
            //Falls es eine subcat ist, dann wird es später nicht in der titles_article_content.csv gefunden
            if( cl_type.equals("page") || cl_type.equals("null") )
            {
               List<String> articlesForLevel = levelArticlesMap.get(level);
               if( articlesForLevel == null )
               {
                  articlesForLevel = new ArrayList<>();
               }
               articlesForLevel.add(page_title);

               levelArticlesMap.put(level, articlesForLevel);
            }
            else if( cl_type.equals("subcat") )
            {
               //Subkategorien können von verschiedenen Artikeln/Subkategorien verlinkt sein, somit keine doppelte Ausführung
               if( !coveredCategories.contains(page_title) )
               {
                  coveredCategories.add(page_title);

                  Dataset<Row> resultSet = joinedCsv.where("cl_to = '" + page_title + "'");
                  List<String> list = resultSet.map((MapFunction<Row, String>) entry -> entry.mkString(";"), Encoders.STRING()).collectAsList();

                  levelArticlesMap = collectLevelArticlesMap(joinedCsv, list, levelArticlesMap, coveredCategories, level + 1);
               }
            }
         }
      }

      return levelArticlesMap;
   }
}
