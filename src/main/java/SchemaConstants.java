import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SchemaConstants
{
   public static StructType pageSchema;
   public static StructType categoryLinksSchema;
   public static StructType articlesSchema;
   public static StructType joinedTableSchema;
   public static StructType levelTitleSchema;

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

      categoryLinksSchema = new StructType(new StructField[]{
            DataTypes.createStructField("cl_from", DataTypes.StringType, true),
            DataTypes.createStructField("cl_to", DataTypes.StringType, true),
            DataTypes.createStructField("cl_sortkey", DataTypes.StringType, true),
            DataTypes.createStructField("cl_timestamp", DataTypes.StringType, true),
            DataTypes.createStructField("cl_sortkey_prefix", DataTypes.StringType, true),
            DataTypes.createStructField("cl_collation", DataTypes.StringType, true),
            DataTypes.createStructField("cl_type", DataTypes.StringType, true)
      });

      articlesSchema = new StructType(new StructField[]{
            DataTypes.createStructField("articleTitle", DataTypes.StringType, true),
            DataTypes.createStructField("articleContent", DataTypes.StringType, true)
      });

      joinedTableSchema = new StructType(new StructField[]{
            DataTypes.createStructField("page_id", DataTypes.StringType, true),
            DataTypes.createStructField("page_namespace", DataTypes.StringType, true),
            DataTypes.createStructField("page_title", DataTypes.StringType, true),
            DataTypes.createStructField("cl_from", DataTypes.StringType, true),
            DataTypes.createStructField("cl_to", DataTypes.StringType, true),
            DataTypes.createStructField("cl_type", DataTypes.StringType, true)
      });

      levelTitleSchema = new StructType(new StructField[]{
            DataTypes.createStructField("level", DataTypes.StringType, true),
            DataTypes.createStructField("titles", DataTypes.StringType, true)
      });
   }
}
