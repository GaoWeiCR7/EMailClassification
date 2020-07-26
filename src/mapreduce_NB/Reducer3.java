package mapreduce_NB;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Reducer3 extends Reducer<Text, Text, Text, Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        if(key.toString().equals("!")){
            Text str = null;
            for(Text val : values){
                str = val;
            }
            context.write(str, str);
        }
        else
        {
            StringBuilder feature = new StringBuilder();
            for(Text wordtfidf:values)
            {
                String v = wordtfidf.toString();
                feature.append(v.split("#")[0]+":"+v.split("#")[1]+" ");
            }
            context.write(key,new Text(feature.toString()));
        }
    }
}
