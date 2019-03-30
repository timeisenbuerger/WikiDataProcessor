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
      csvDumper.dumpAllDumpDataInCsv();
   }

   /**
    * Artikeltexte formatieren und in eine CSV Datei schreiben.
    * Nur mit einer Sparkpartition, da sonst die Artikeltexte getrennt werden und nicht mehr zuzuordnen sind
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
   public void testDumpProcessor() throws IOException
   {
      DumpProcessor dumpAnalyzer = new DumpProcessor();
      dumpAnalyzer.collectArticlesRelatedToAnimals();
   }

   @Test
   public void testCSVDumperNeededTitlesWithContent() throws IOException
   {
      CSVDumper csvDumper = new CSVDumper(true);
      csvDumper.dumpNeededTitlesWithContent();
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
