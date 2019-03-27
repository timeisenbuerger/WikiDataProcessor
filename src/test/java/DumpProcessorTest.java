import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Test;

public class DumpProcessorTest
{
   @Test
   public void testCSVDumperAllData() throws IOException
   {
      CSVDumper csvDumper = new CSVDumper(true);
      csvDumper.dumpAllDataInCsv();
   }

   @Test
   public void testCSVDumperArticleTextAsCsv() throws IOException
   {
      CSVDumper csvDumper = new CSVDumper(false);
      csvDumper.dumpArticleTextAsCsv();
   }

   @Test
   public void testDumpAnalyzer()
   {
      DumpAnalyzer dumpAnalyzer = new DumpAnalyzer();
      dumpAnalyzer.analyzeArticlesRelatedToAnimals();
   }

   @Test
   public void testArticlesAnalyzer() throws FileNotFoundException
   {
      ArticlesAnalyzer articlesAnalyzer = new ArticlesAnalyzer();
      articlesAnalyzer.analyzeArticleTexts();
   }

   @Test
   public void merge() throws IOException
   {
      ArticlesAnalyzer articlesAnalyzer = new ArticlesAnalyzer();
      articlesAnalyzer.merge();
   }
}
