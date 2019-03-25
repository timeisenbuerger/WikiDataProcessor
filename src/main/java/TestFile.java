import edu.stanford.nlp.simple.Sentence;

import java.util.List;

public class TestFile
{
    public static void main(String[] args)
    {
        Sentence sentence = new Sentence("I work at Google and live in New York. My dog is called Lucy.");
        List<String> nerTags = sentence.nerTags();
        for (String posTag : nerTags)
        {
            System.out.print(posTag + ", ");
        }
    }
}
