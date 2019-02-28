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
   private static final String CSV_DIRECTORY = "E:\\Entwicklung\\WikiDumps\\csv\\";

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

   public void dumpAllDataInCsv() throws IOException
   {
      String pagePath = CSV_DIRECTORY + "page.csv";
      String categoryLinksPath = CSV_DIRECTORY + "categorylinks.csv";

      SparkConf sparkConf = new SparkConf().setAppName("dumpData").setMaster("local[*]");
      sc = new SparkContext(sparkConf);

      sqlC = new SQLContext(new SparkSession(sc));

      Dataset<Row> pageCsv = sqlC.read().schema(pageSchema).csv(pagePath);
      Dataset<Row> categoryLinksCsv = sqlC.read().schema(categoryLinksSchema).csv(categoryLinksPath);

      pageCsv = pageCsv.select("page_id", "page_namespace", "page_title");
      categoryLinksCsv = categoryLinksCsv.select("cl_from", "cl_to", "cl_type");

      Dataset<Row> joinedTable = pageCsv.join(categoryLinksCsv, pageCsv.col("page_id").equalTo(categoryLinksCsv.col("cl_from")));

      FileSystem fileSystem = FileSystem.get(sc.hadoopConfiguration());

      dumpPageCategoryLinksJoin(joinedTable, fileSystem);
      dumpOnlySubcategories(joinedTable, fileSystem);
      dumpOnlyArticleRelations(joinedTable, fileSystem);
   }

   private void dumpPageCategoryLinksJoin(Dataset<Row> joinedTable, FileSystem fileSystem) throws IOException
   {
      joinedTable.write().csv(CSV_DIRECTORY + "joined.csv");
      FileUtil.copyMerge(fileSystem, new Path(CSV_DIRECTORY + "joined.csv"), fileSystem,
            new Path(CSV_DIRECTORY + "page_categorylinks_joined.csv"), true, sc.hadoopConfiguration(), null
      );
   }

   private void dumpOnlySubcategories(Dataset<Row> joinedTable, FileSystem fileSystem) throws IOException
   {
      Dataset<Row> onlySubcategories = joinedTable.where("page_namespace = 14");

      onlySubcategories.write().csv(CSV_DIRECTORY + "subcategories.csv");
      FileUtil.copyMerge(fileSystem, new Path(CSV_DIRECTORY + "subcategories.csv"), fileSystem,
            new Path(CSV_DIRECTORY + "only_subcategories.csv"), true, sc.hadoopConfiguration(), null
      );
   }

   private void dumpOnlyArticleRelations(Dataset<Row> joinedTable, FileSystem fileSystem) throws IOException
   {
      Dataset<Row> onlyArticleRelations = joinedTable.where("page_namespace = 0");

      onlyArticleRelations.write().csv(CSV_DIRECTORY + "article_relations.csv");
      FileUtil.copyMerge(fileSystem, new Path(CSV_DIRECTORY + "article_relations.csv"), fileSystem,
            new Path(CSV_DIRECTORY + "only_article_relations.csv"), true, sc.hadoopConfiguration(), null
      );
   }
}
