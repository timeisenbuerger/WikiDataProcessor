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
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
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

public class CSVDumper implements Serializable
{
   private SparkContext sparkContext;
   private SQLContext sqlContext;
   private SparkConf sparkConf;

   private String pagePath;
   private String categoryLinksPath;

   public CSVDumper(boolean allData)
   {
      init(allData);
   }

   private void init(boolean allData)
   {
      pagePath = PathConstants.CSV_DIRECTORY + "page.csv";
      categoryLinksPath = PathConstants.CSV_DIRECTORY + "categorylinks.csv";

      if( allData )
      {
         sparkConf = new SparkConf().setAppName("dumpData").setMaster("local[*]");
      }
      else
      {
         sparkConf = new SparkConf().setAppName("dumpData").setMaster("local[1]");
      }

      sparkContext = new SparkContext(sparkConf);
      sqlContext = new SQLContext(new SparkSession(sparkContext));
   }

   public void dumpAllDumpDataInCsv() throws IOException
   {
      Dataset<Row> pageCsv = sqlContext.read().schema(SchemaConstants.pageSchema).csv(pagePath);
      Dataset<Row> categoryLinksCsv = sqlContext.read().schema(SchemaConstants.categoryLinksSchema).csv(categoryLinksPath);

      pageCsv = pageCsv.select("page_id", "page_namespace", "page_title");
      categoryLinksCsv = categoryLinksCsv.select("cl_from", "cl_to", "cl_type");

      Dataset<Row> joinedTable = pageCsv.join(categoryLinksCsv, pageCsv.col("page_id").equalTo(categoryLinksCsv.col("cl_from")));

      FileSystem fileSystem = FileSystem.get(sparkContext.hadoopConfiguration());

      dumpPageCategoryLinksJoin(joinedTable, fileSystem);
      dumpOnlySubcategories(joinedTable, fileSystem);
      dumpOnlyArticleRelations(joinedTable, fileSystem);
   }

   private void dumpPageCategoryLinksJoin(Dataset<Row> joinedTable, FileSystem fileSystem) throws IOException
   {
      joinedTable.write().csv(PathConstants.CSV_DIRECTORY + "joined.csv");
      FileUtil.copyMerge(fileSystem, new Path(PathConstants.CSV_DIRECTORY + "joined.csv"), fileSystem,
            new Path(PathConstants.CSV_DIRECTORY + "page_categorylinks_joined.csv"), true, sparkContext.hadoopConfiguration(), null
      );
   }

   private void dumpOnlySubcategories(Dataset<Row> joinedTable, FileSystem fileSystem) throws IOException
   {
      Dataset<Row> onlySubcategories = joinedTable.where("page_namespace = 14");

      onlySubcategories.write().csv(PathConstants.CSV_DIRECTORY + "subcategories.csv");
      FileUtil.copyMerge(fileSystem, new Path(PathConstants.CSV_DIRECTORY + "subcategories.csv"), fileSystem,
            new Path(PathConstants.CSV_DIRECTORY + "only_subcategories.csv"), true, sparkContext.hadoopConfiguration(), null
      );
   }

   private void dumpOnlyArticleRelations(Dataset<Row> joinedTable, FileSystem fileSystem) throws IOException
   {
      Dataset<Row> onlyArticleRelations = joinedTable.where("page_namespace = 0");

      onlyArticleRelations.write().csv(PathConstants.CSV_DIRECTORY + "article_relations.csv");
      FileUtil.copyMerge(fileSystem, new Path(PathConstants.CSV_DIRECTORY + "article_relations.csv"), fileSystem,
            new Path(PathConstants.CSV_DIRECTORY + "only_article_relations.csv"), true, sparkContext.hadoopConfiguration(), null
      );
   }

   public void dumpArticleTextAsCsv() throws IOException
   {
      Dataset<Row> rows = sqlContext.read().text(PathConstants.ARTICLE_TEXT_DIRECTORY + "articles_in_plain_text.txt");

      rows.map((MapFunction<Row, String>) entry -> entry.mkString(";"), Encoders.STRING()).foreachPartition(
            new ForeachPartitionFunction<String>()
            {
               @Override
               public void call(Iterator<String> t) throws Exception
               {
                  File dir = new File(PathConstants.ARTICLE_TEXT_DIRECTORY + "article_text_csvs\\");
                  if( !dir.exists() )
                  {
                     dir.mkdir();
                  }
                  int count = dir.listFiles().length;

                  System.out.println("Schreibe Datei " + count);

                  File file = new File(PathConstants.ARTICLE_TEXT_DIRECTORY + "article_text_csvs\\article_text" + count + ".csv");

                  BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file));
                  String articleTitle = "";
                  String articleContent = "";
                  CSVPrinter csvPrinter = new CSVPrinter(bufferedWriter, CSVFormat.newFormat(';'));

                  while( t.hasNext() )
                  {
                     String line = t.next();
                     if( line.isEmpty() || line.startsWith("#Subtitle") || line.startsWith("#Type") )
                     {
                        continue;
                     }

                     if( line.startsWith("#Article") )
                     {
                        if( articleTitle.isEmpty() )
                        {
                           articleTitle += line.replace("#Article: ", "");
                        }
                        else
                        {
                           csvPrinter.printRecord(articleTitle, articleContent);
                           bufferedWriter.newLine();

                           articleTitle = line.replace("#Article: ", "");
                           articleContent = "";
                        }
                     }
                     else
                     {
                        articleContent += line.replace(";", "").trim();
                     }
                  }

                  csvPrinter.flush();
                  csvPrinter.close();
               }
            });

      fixPartitionFiles(true);
      fixPartitionFiles(false);

      System.out.println("Dateien wieder zusammenfassen");

      FileSystem fileSystem = FileSystem.get(sparkContext.hadoopConfiguration());
      FileUtil.copyMerge(fileSystem, new Path(PathConstants.ARTICLE_TEXT_DIRECTORY + "article_text_csvs"), fileSystem,
            new Path(PathConstants.ARTICLE_TEXT_DIRECTORY + "article_texts.csv"), true, sparkContext.hadoopConfiguration(), null
      );
   }

   private void fixPartitionFiles(boolean isFirstPass) throws IOException
   {
      File dir = new File(PathConstants.ARTICLE_TEXT_DIRECTORY + "article_text_csvs\\");
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

   public void dumpNeededTitlesWithContent() throws IOException
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

         sqlContext.read().schema(SchemaConstants.articlesSchema)
               .option("delimiter", ";")
               .csv(PathConstants.ARTICLE_TEXT_DIRECTORY + "article_texts.csv")
               .filter((Row x) -> titles.contains(x.get(0)))
               .dropDuplicates()
               .write()
               .option("delimiter", ";")
               .csv(PathConstants.CSV_DIRECTORY + "neededArticleTitlesAndContents.csv");

         FileSystem fileSystem = FileSystem.get(sparkContext.hadoopConfiguration());
         FileUtil.copyMerge(fileSystem, new Path(PathConstants.CSV_DIRECTORY + "neededArticleTitlesAndContents.csv"), fileSystem,
               new Path(PathConstants.CSV_DIRECTORY + "needed_article_titles_and_contents.csv"), true, sparkContext.hadoopConfiguration(), null
         );
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
