import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
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

   private SparkConf sparkConf;

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

      Dataset<Row> animals = joinedCsv.where("cl_from = 'Animals'");
      animals.show();
   }
}
