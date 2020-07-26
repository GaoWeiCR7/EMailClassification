package mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.DoubleWritable;
import preprocess.TextTokenizer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class EMailMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private Text word = new Text();
    private DoubleWritable count = new DoubleWritable(1);
    List<String> list = new LinkedList<>();
    String fileName;

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        List<String> templist = TextTokenizer.getInstance().tokenize(value.toString());
        list.addAll(templist);
        fileName = fileSplit.getPath().getName();
        for (String string : templist) {
            word.set(string+'#'+fileName);
            context.write(word, count);
        }
    }

    public void cleanup(Context context) throws IOException, InterruptedException
    {
        word.set("!#"+fileName);
        context.write(word,new DoubleWritable(list.size()));
    }
}
