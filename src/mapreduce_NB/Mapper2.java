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

public class Mapper2 extends Mapper<Object, Text, Text, DoubleWritable>{
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        String temp = value.toString();
        context.write(new Text(temp.split("\t")[0]),new DoubleWritable(1));
    }
}
