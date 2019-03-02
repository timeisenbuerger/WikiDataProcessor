import java.io.IOException;

import org.junit.Test;

public class DumpProcessorTest
{
   @Test
   public void testCSVDumper() throws IOException
   {
      CSVDumper csvDumper = new CSVDumper();
      csvDumper.dumpAllDataInCsv();
   }

   @Test
   public void testDumpAnalyzer()
   {
      DumpAnalyzer dumpAnalyzer = new DumpAnalyzer();

   }
}
