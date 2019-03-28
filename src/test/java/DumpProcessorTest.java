import java.io.IOException;

import org.junit.Test;

public class DumpProcessorTest
{
   /**
    * CSV dumps erstellen
    *
    * @throws IOException
    */

   @Test
   public void testCSVDumperAllData() throws IOException
   {
      CSVDumper csvDumper = new CSVDumper(true);
      csvDumper.dumpAllDataInCsv();
   }

   /**
    * Artikeltexte formatieren und in eine CSV Datei schreiben
    *
    * @throws IOException
    */

   @Test
   public void testCSVDumperArticleTextAsCsv() throws IOException
   {
      CSVDumper csvDumper = new CSVDumper(false);
      csvDumper.dumpArticleTextAsCsv();
   }

   /**
    * Zuordnung von Titeln und Leveln in Datei schreiben
    *
    */

   @Test
   public void testDumpAnalyzer() throws IOException
   {
      DumpAnalyzer dumpAnalyzer = new DumpAnalyzer();
      dumpAnalyzer.analyzeArticlesRelatedToAnimals();
   }

   /**
    *
    * Artikeltexte analysieren
    *
    * @throws IOException
    */

   @Test
   public void testArticlesAnalyzer() throws IOException
   {
      ArticlesAnalyzer articlesAnalyzer = new ArticlesAnalyzer();
      articlesAnalyzer.analyzeArticleTexts();
   }
}
