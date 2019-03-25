import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

public class DumpAnalyzer
{
//   private static final String CSV_DIRECTORY = "E:\\Entwicklung\\WikiDumps\\csv\\";
//   private static final String ARTICLE_TEXT_DIRECTORY = "E:\\Entwicklung\\WikiDumps\\article_xml\\extracted_text\\";
   private static final String CSV_DIRECTORY = "D:\\Uni\\05-ws1819\\PTT\\wikidata\\csv\\";
   private static final String ARTICLE_TEXT_DIRECTORY = "D:\\Uni\\05-ws1819\\PTT\\wikidata\\extracted_text\\";

   private static StructType joinedTableSchema;

   private static SparkContext sc;
   private static SQLContext sqlC;

   static
   {
      joinedTableSchema = new StructType(new StructField[]{
            DataTypes.createStructField("page_id", DataTypes.StringType, true),
            DataTypes.createStructField("page_namespace", DataTypes.StringType, true),
            DataTypes.createStructField("page_title", DataTypes.StringType, true),
            DataTypes.createStructField("cl_from", DataTypes.StringType, true),
            DataTypes.createStructField("cl_to", DataTypes.StringType, true),
            DataTypes.createStructField("cl_type", DataTypes.StringType, true)
      });
   }

   //Page id von Animals: 21143238

   private SparkConf sparkConf;

   public DumpAnalyzer()
   {
      init();
   }

   private void init()
   {
      sparkConf = new SparkConf().setAppName("analyzeData").setMaster("local[*]");
      sc = new SparkContext(sparkConf);
      sqlC = new SQLContext(new SparkSession(sc));
   }

   public void analyzeArticlesRelatedToAnimals()
   {
      Dataset<Row> joinedCsv = sqlC.read().schema(joinedTableSchema).csv(CSV_DIRECTORY + "page_categorylinks_joined.csv");
      joinedCsv.cache();
      Dataset<Row> animals = joinedCsv.where("cl_to = 'Animals'");

      List<String> resultList = animals.map((MapFunction<Row, String>) entry -> entry.mkString(";"), Encoders.STRING()).collectAsList();

      //Zuordnung von Artikeln zu Ebenen
      Map<Integer, List<String>> levelArticlesMap = new HashMap<>();
      //Wird genutzt, um bereits abgedeckte Subkategorien zu überspringen
      List<String> coveredCategories = new ArrayList<>();
      int level = 0;

      levelArticlesMap = collectLevelArticlesMap(joinedCsv, resultList, levelArticlesMap, coveredCategories, level);

      for( Map.Entry<Integer, List<String>> entry : levelArticlesMap.entrySet() )
      {
         System.out.println(entry.getKey() + ":");
         for( String s : entry.getValue() )
         {
            System.out.println(s);
         }
      }
   }

   private Map<Integer, List<String>> collectLevelArticlesMap(Dataset<Row> joinedCsv, List<String> resultList, Map<Integer, List<String>> levelArticlesMap, List<String> coveredCategories, int level)
   {
      if(level < 3)
      {
         for( String row : resultList )
         {
            List<String> columnValues = Arrays.asList(row.split(";"));
            String page_title = columnValues.get(2);
            String cl_type = columnValues.get(5);

            //Dirty workaround, da es folgende Titel geben kann: Animals_of_Kha'ir. Würde eine SQL-Exception hervorrufen
            if( page_title.contains("'") )
            {
               continue;
            }

            if( cl_type.equals("page") )
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
            else if( cl_type.equals("null") )
            {
               List<String> articlesForLevel = levelArticlesMap.get(level);
               if( articlesForLevel == null )
               {
                  articlesForLevel = new ArrayList<>();
               }
               articlesForLevel.add(page_title);

               levelArticlesMap.put(-1, articlesForLevel);
            }
         }
      }

      return levelArticlesMap;
   }
}
