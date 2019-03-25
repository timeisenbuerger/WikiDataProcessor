import edu.stanford.nlp.simple.Sentence;

import java.util.List;

public class TestFile
{
    public static void main(String[] args)
    {
        Sentence sentence = new Sentence("I always loved bagels, I used to eat them in New York.");
        List<String> posTags = sentence.posTags();
        for (String posTag : posTags)
        {
            System.out.print(posTag + ", ");
        }
    }
}
