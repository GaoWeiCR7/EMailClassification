import mapreduce.EMailMapper2;
import mapreduce.EMailReducer2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mockito.internal.matchers.Null;

import java.io.File;
import java.io.IOException;
import java.net.URI;

public class knnmain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("args length wrong\n");
            System.exit(2);
        }
        Job knnjob = Job.getInstance(conf, "knn");
        knnjob.setJarByClass(knnmain.class);
        knnjob.setMapperClass(classification.knnmap.class);
        knnjob.setProfileParams(args[0]);
        knnjob.setMapOutputKeyClass(Text.class);
        knnjob.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(knnjob,new Path(args[1]));
        FileOutputFormat.setOutputPath(knnjob,new Path(args[2]));

        knnjob.waitForCompletion(true);
    }
}
