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
   private static final String CSV_DIRECTORY = "E:\\Entwicklung\\WikiDumps\\csv\\";
   private static final String ARTICLE_TEXT_DIRECTORY = "E:\\Entwicklung\\WikiDumps\\article_xml\\extracted_text\\";

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

      Map<Integer, List<String>> levelArticlesMap = new HashMap<>();
      int level = 0;

      levelArticlesMap = collectLevelArticlesMap(joinedCsv, resultList, levelArticlesMap, level);

      System.out.println(levelArticlesMap);
   }

   private Map<Integer, List<String>> collectLevelArticlesMap(Dataset<Row> joinedCsv, List<String> resultList, Map<Integer, List<String>> levelArticlesMap, int level)
   {
      for( String row : resultList )
      {
         List<String> columnValues = Arrays.asList(row.split(";"));
         String page_title = columnValues.get(2);
         String cl_to = columnValues.get(4);
         String cl_type = columnValues.get(5);

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
            Dataset<Row> resultSet = joinedCsv.where("cl_to = '" + cl_to + "'");
            List<String> list = resultSet.map((MapFunction<Row, String>) entry -> entry.mkString(";"), Encoders.STRING()).collectAsList();

            levelArticlesMap = collectLevelArticlesMap(joinedCsv, list, levelArticlesMap, level + 1);
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

      return levelArticlesMap;
   }
}
