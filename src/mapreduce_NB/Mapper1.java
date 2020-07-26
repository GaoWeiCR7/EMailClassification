package mapreduce_NB;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.DoubleWritable;
import preprocess.TextTokenizer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class Mapper1 extends Mapper<Object, Text, Text, DoubleWritable>{
    private Text word = new Text();
    private DoubleWritable count = new DoubleWritable(1);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        List<String> templist = TextTokenizer.getInstance().tokenize(value.toString());
        for (String string : templist) {
            word.set(string+'#'+fileName);
            context.write(word, count);
        }
    }
}
