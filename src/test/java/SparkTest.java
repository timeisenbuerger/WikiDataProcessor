import java.io.IOException;

import org.junit.Test;

public class SparkTest
{
   @Test
   public void test() throws IOException
   {
      CSVDumper csvDumper = new CSVDumper();
      csvDumper.dumpAllDataInCsv();
   }
}
