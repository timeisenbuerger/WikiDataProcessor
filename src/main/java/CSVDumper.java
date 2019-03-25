import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CSVDumper implements Serializable
{
//   private static final String CSV_DIRECTORY = "E:\\Entwicklung\\WikiDumps\\csv\\";
//   private static final String ARTICLE_TEXT_DIRECTORY = "E:\\Entwicklung\\WikiDumps\\article_xml\\extracted_text\\";
   private static final String CSV_DIRECTORY = "D:\\Uni\\05-ws1819\\PTT\\wikidata\\csv\\";
   private static final String ARTICLE_TEXT_DIRECTORY = "D:\\Uni\\05-ws1819\\PTT\\wikidata\\extracted_text\\";

   private static SparkContext sc;
   private static SQLContext sqlC;

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

   private String pagePath;
   private String categoryLinksPath;
   private SparkConf sparkConf;

   public CSVDumper(boolean allData)
   {
      init(allData);
   }

   private void init(boolean allData)
   {
      pagePath = CSV_DIRECTORY + "page.csv";
      categoryLinksPath = CSV_DIRECTORY + "categorylinks.csv";

      if( allData )
      {
         sparkConf = new SparkConf().setAppName("dumpData").setMaster("local[*]");
      }
      else
      {
         sparkConf = new SparkConf().setAppName("dumpData").setMaster("local[1]");
      }

      sc = new SparkContext(sparkConf);
      sqlC = new SQLContext(new SparkSession(sc));
   }

   public void dumpAllDataInCsv() throws IOException
   {
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

   public void dumpArticleTextAsCsv() throws IOException
   {
      Dataset<Row> rows = sqlC.read().text(ARTICLE_TEXT_DIRECTORY + "articles_in_plain_text.txt");

      rows.map((MapFunction<Row, String>) entry -> entry.mkString(";"), Encoders.STRING()).foreachPartition(
            new ForeachPartitionFunction<String>()
            {
               @Override
               public void call(Iterator<String> t) throws Exception
               {
                  File dir = new File(ARTICLE_TEXT_DIRECTORY + "article_text_csvs\\");
                  if( !dir.exists() )
                  {
                     dir.mkdir();
                  }
                  int count = dir.listFiles().length;

                  System.out.println("Schreibe Datei " + count);

                  File file = new File(ARTICLE_TEXT_DIRECTORY + "article_text_csvs\\article_text" + count + ".csv");


                  BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file));
                  String articleLine = "";

                  while( t.hasNext() )
                  {
                     String line = t.next();
                     if( line.isEmpty() || line.startsWith("#Subtitle") || line.startsWith("#Type") )
                     {
                        continue;
                     }

                     if( line.startsWith("#Article") )
                     {
                        if( articleLine.isEmpty() )
                        {
                           articleLine += line.replace("#Article: ", "") + ";";
                        }
                        else
                        {
                           bufferedWriter.write(articleLine);
                           bufferedWriter.newLine();

                           articleLine = line.replace("#Article: ", "") + ";";
                        }
                     }
                     else
                     {
                        articleLine += line.replace(";", "").trim();
                     }
                  }
                  bufferedWriter.flush();
               }
            });

      fixPartitionFiles(true);
      fixPartitionFiles(false);
   }

   private void fixPartitionFiles(boolean isFirstPass) throws IOException
   {
      File dir = new File(ARTICLE_TEXT_DIRECTORY + "article_text_csvs\\");
      if( dir.exists() )
      {
         if( isFirstPass )
         {
            System.out.println("Erster Durchlauf");

            List<File> files = Arrays.asList(dir.listFiles());
            for( int i = 0; i < files.size(); i += 2 )
            {
               System.out.println("Bearbeitung von " + i + " und " + (i + 1));

               File fileTo = files.get(i);
               File fileFrom = files.get(i + 1);

               List<String> linesToMove = new ArrayList<>();
               List<String> linesFileFrom = readLines(fileFrom);
               for( String line : linesFileFrom )
               {
                  if( line.contains(";") )
                  {
                     break;
                  }
                  else
                  {
                     linesToMove.add(line);
                  }
               }

               linesFileFrom.removeAll(linesToMove);

               List<String> linesFileTo = readLines(fileTo);
               String lastLine = linesFileTo.get(linesFileTo.size() - 1);
               for( int j = 0; j < linesToMove.size(); j++ )
               {
                  String lineToMove = linesToMove.get(j);
                  if( j == 0 )
                  {
                     lastLine += " " + lineToMove;
                  }
                  else
                  {
                     lastLine += linesToMove;
                  }
               }
               linesFileTo.set(linesFileTo.size() - 1, lastLine);

               System.out.println("Datei 1 neu beschreiben");

               BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(fileTo));
               rewriteFile(linesFileTo, bufferedWriter);

               System.out.println("Datei 2 neu beschreiben");

               bufferedWriter = new BufferedWriter(new FileWriter(fileFrom));
               rewriteFile(linesFileFrom, bufferedWriter);
            }
         }
         else
         {
            System.out.println("Zweiter Durchlauf");

            List<File> files = Arrays.asList(dir.listFiles());
            for( int i = 2; i < files.size(); i += 2 )
            {
               System.out.println("Bearbeitung von " + i + " und " + (i + 1));

               File fileFrom = files.get(i);
               File fileTo = files.get(i - 1);

               List<String> linesToMove = new ArrayList<>();
               List<String> linesFileFrom = readLines(fileTo);
               for( String line : linesFileFrom )
               {
                  if( line.contains(";") )
                  {
                     break;
                  }
                  else
                  {
                     linesToMove.add(line);
                  }
               }

               linesFileFrom.removeAll(linesToMove);

               List<String> linesFileTo = readLines(fileTo);
               String lastLine = linesFileTo.get(linesFileTo.size() - 1);
               for( int j = 0; j < linesToMove.size(); j++ )
               {
                  String lineToMove = linesToMove.get(j);
                  if( j == 0 )
                  {
                     lastLine += " " + lineToMove;
                  }
                  else
                  {
                     lastLine += linesToMove;
                  }
               }
               linesFileTo.set(linesFileTo.size() - 1, lastLine);

               System.out.println("Datei 1 neu beschreiben");

               BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(fileTo));
               rewriteFile(linesFileTo, bufferedWriter);

               System.out.println("Datei 2 neu beschreiben");

               bufferedWriter = new BufferedWriter(new FileWriter(fileFrom));
               rewriteFile(linesFileFrom, bufferedWriter);
            }
         }
      }
   }

   private void rewriteFile(List<String> linesFile1, BufferedWriter bufferedWriter) throws IOException
   {
      for( int j = 0; j < linesFile1.size(); j++ )
      {
         String line = linesFile1.get(j);
         bufferedWriter.write(line);

         if( j != linesFile1.size() - 1 )
         {
            bufferedWriter.newLine();
         }
      }
      bufferedWriter.flush();
   }

   private List<String> readLines(File file) throws IOException
   {
      if( !file.exists() )
      {
         return new ArrayList<>();
      }
      BufferedReader reader = new BufferedReader(new FileReader(file));
      List<String> results = new ArrayList<>();
      String line = reader.readLine();
      while( line != null )
      {
         results.add(line);
         line = reader.readLine();
      }
      return results;
   }

}
