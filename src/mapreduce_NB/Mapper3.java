package mapreduce_NB;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import preprocess.TextTokenizer;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class Mapper3 extends Mapper<Object, Text, Text, Text>{
    List<String> wordList = new ArrayList<String>();
    List<Double> idfList = new ArrayList<Double>();
    public void setup(Context context) throws IOException, InterruptedException{
        int allwordnum = 0;
        int allwordkind = 0;
        String filefolder = context.getProfileParams();
        FileSystem temp = FileSystem.get(URI.create(filefolder),context.getConfiguration());
        FileStatus[] res = temp.listStatus(new Path(filefolder));
        Path []paths =  FileUtil.stat2Paths(res);
        for(Path path:paths)
        {
            FSDataInputStream inStream = FileSystem.get(context.getConfiguration()).open(path);
            while(inStream.available() > 0)
            {
                String linestr = inStream.readLine();
                wordList.add(linestr.split("\t")[0]);
                allwordnum += Integer.parseInt(linestr.split("\t")[1]);
                allwordkind++;
            }
        }
        context.write(new Text("!"), new Text(String.valueOf(allwordnum)+"#"+String.valueOf(allwordkind)));
    }
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        String linestr = value.toString();

        String word = linestr.split("\t")[0];
        int tf = Integer.parseInt(linestr.split("\t")[1]);

        int index = wordList.indexOf(word);

        if(index!=-1)
            context.write(new Text(fileName),new Text(index + "#" + tf));
    }
}
