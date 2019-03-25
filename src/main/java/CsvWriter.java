import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

class CsvWriter
{
    void createCsvFromMap(Map<String, Integer> map, String filepath)
    {
        FileWriter fileWriter = null;
        try
        {
            fileWriter = new FileWriter(filepath);

            for (String key : map.keySet())
            {
                fileWriter.append(key);
                fileWriter.append(",");
                fileWriter.append(map.get(key).toString());
                fileWriter.append("\n");
            }

        } catch (IOException e)
        {
            e.printStackTrace();
        }
        finally
        {
            try
            {
                fileWriter.flush();
                fileWriter.close();

            } catch (IOException e)
            {
                e.printStackTrace();
            }
        }
    }
}