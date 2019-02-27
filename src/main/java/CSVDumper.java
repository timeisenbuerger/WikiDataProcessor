import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CSVDumper
{
   private static SparkContext sc;
   private static SQLContext sqlC;
   private static JavaSparkContext jsc;

   private static StructType pageSchema;
   private static StructType categoryLinksSchema;

   static
   {
      pageSchema = new StructType(new StructField[]{
            DataTypes.createStructField("page_id", DataTypes.StringType, true),
            DataTypes.createStructField("page_namespace", DataTypes.StringType, true),
            DataTypes.createStructField("page_title", DataTypes.StringType, true),
            DataTypes.createStructField("page_restrictions", DataTypes.StringType, true),
            DataTypes.createStructField("page_is_redirect", DataTypes.StringType, true),
            DataTypes.createStructField("page_is_new", DataTypes.StringType, true),
            DataTypes.createStructField("page_random", DataTypes.StringType, true),
            DataTypes.createStructField("page_touched", DataTypes.StringType, true),
            DataTypes.createStructField("page_links_updated", DataTypes.StringType, true),
            DataTypes.createStructField("page_latest", DataTypes.StringType, true),
            DataTypes.createStructField("page_len", DataTypes.StringType, true),
            DataTypes.createStructField("page_content_model", DataTypes.StringType, true),
            DataTypes.createStructField("page_lang", DataTypes.StringType, true)
      });
   }

   static
   {
      categoryLinksSchema = new StructType(new StructField[]{
            DataTypes.createStructField("cl_from", DataTypes.StringType, true),
            DataTypes.createStructField("cl_to", DataTypes.StringType, true),
            DataTypes.createStructField("cl_sortkey", DataTypes.StringType, true),
            DataTypes.createStructField("cl_timestamp", DataTypes.StringType, true),
            DataTypes.createStructField("cl_sortkey_prefix", DataTypes.StringType, true),
            DataTypes.createStructField("cl_collation", DataTypes.StringType, true),
            DataTypes.createStructField("cl_type", DataTypes.StringType, true)
      });
   }

   public void dumpPageCategoryLinksJoin() throws IOException
   {
      String pagePath = "E:\\Entwicklung\\WikiDumps\\csv\\page.csv";
      String categoryLinksPath = "E:\\Entwicklung\\WikiDumps\\csv\\categorylinks.csv";

      SparkConf sparkConf = new SparkConf().setAppName("joinTables").setMaster("local[*]");
      sc = new SparkContext(sparkConf);

      sqlC = new SQLContext(new SparkSession(sc));

      Dataset<Row> pageCsv = sqlC.read().schema(pageSchema).csv(pagePath);
      Dataset<Row> categoryLinksCsv = sqlC.read().schema(categoryLinksSchema).csv(categoryLinksPath);

      Dataset<Row> joinedTable = pageCsv.join(categoryLinksCsv, pageCsv.col("page_id").equalTo(categoryLinksCsv.col("cl_from")));

      joinedTable.write().csv("E:\\Entwicklung\\WikiDumps\\csv\\joined.csv");
      FileSystem fileSystem = FileSystem.get(sc.hadoopConfiguration());
      FileUtil.copyMerge(fileSystem, new Path("E:\\Entwicklung\\WikiDumps\\csv\\joined.csv"), fileSystem,
            new Path("E:\\Entwicklung\\WikiDumps\\csv\\merged.csv"), true, sc.hadoopConfiguration(), null
      );
   }

   public void dumpOnlySubcategories() throws IOException
   {
      String pagePath = "E:\\Entwicklung\\WikiDumps\\csv\\page.csv";
      String categoryLinksPath = "E:\\Entwicklung\\WikiDumps\\csv\\categorylinks.csv";

      SparkConf sparkConf = new SparkConf().setAppName("onlySubcategories").setMaster("local[*]");
      sc = new SparkContext(sparkConf);

      sqlC = new SQLContext(new SparkSession(sc));

      Dataset<Row> pageCsv = sqlC.read().schema(pageSchema).csv(pagePath);
      Dataset<Row> categoryLinksCsv = sqlC.read().schema(categoryLinksSchema).csv(categoryLinksPath);

      Dataset<Row> onlySubcategories = pageCsv.join(categoryLinksCsv, pageCsv.col("page_id").equalTo(categoryLinksCsv.col("cl_from")))
            .where("page_namespace = 14");

      onlySubcategories.write().csv("E:\\Entwicklung\\WikiDumps\\csv\\subcategories.csv");
      FileSystem fileSystem = FileSystem.get(sc.hadoopConfiguration());
      FileUtil.copyMerge(fileSystem, new Path("E:\\Entwicklung\\WikiDumps\\csv\\subcategories.csv"), fileSystem,
            new Path("E:\\Entwicklung\\WikiDumps\\csv\\only_subcategories.csv"), true, sc.hadoopConfiguration(), null
      );
   }

   public void dumpOnlyArticleRelations() throws IOException
   {
      String pagePath = "E:\\Entwicklung\\WikiDumps\\csv\\page.csv";
      String categoryLinksPath = "E:\\Entwicklung\\WikiDumps\\csv\\categorylinks.csv";

      SparkConf sparkConf = new SparkConf().setAppName("onlyArticleRelations").setMaster("local[*]");
      sc = new SparkContext(sparkConf);

      sqlC = new SQLContext(new SparkSession(sc));

      Dataset<Row> pageCsv = sqlC.read().schema(pageSchema).csv(pagePath);
      Dataset<Row> categoryLinksCsv = sqlC.read().schema(categoryLinksSchema).csv(categoryLinksPath);

      Dataset<Row> joinedTable = pageCsv.join(categoryLinksCsv, pageCsv.col("page_id").equalTo(categoryLinksCsv.col("cl_from")));

      Dataset<Row> onlyArticleRelations = joinedTable.where("page_namespace = 0");

      onlyArticleRelations.write().csv("E:\\Entwicklung\\WikiDumps\\csv\\article_relations.csv");
      FileSystem fileSystem = FileSystem.get(sc.hadoopConfiguration());
      FileUtil.copyMerge(fileSystem, new Path("E:\\Entwicklung\\WikiDumps\\csv\\article_relations.csv"), fileSystem,
            new Path("E:\\Entwicklung\\WikiDumps\\csv\\only_article_relations.csv"), true, sc.hadoopConfiguration(), null
      );
   }
}
