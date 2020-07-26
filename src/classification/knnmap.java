package classification;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;

import static java.lang.Math.abs;
import static java.lang.Math.sqrt;

public class knnmap extends Mapper<Object, Text, Text, Text>{
    private ArrayList<HashMap<String, Double>> trainfeature = new ArrayList<>();
    private ArrayList<String> trainclass = new ArrayList<>();
    private int k = 5;
    ArrayList<String> allclass = new ArrayList<String>();


    public void setup(Context context) throws IOException, InterruptedException{
        String idffilefolder = context.getProfileParams();
        FileSystem temp = FileSystem.get(URI.create(idffilefolder), context.getConfiguration());
        FileStatus[] res = temp.listStatus(new Path(idffilefolder));
        Path[] paths = FileUtil.stat2Paths(res);
        for(Path p: paths){
            FSDataInputStream inStream = FileSystem.get(context.getConfiguration()).open(p);
            while(inStream.available() > 0){
                String linestr = inStream.readLine();
                String nclass = linestr.split("\t")[0].split("#")[0];
                if(allclass.contains(nclass) == false)
                {
                    allclass.add(nclass);
                }
                trainclass.add(nclass);

                String[] features = linestr.split("\t")[1].split(" ");
                HashMap<String, Double> tempfea = new HashMap<>();
                for(int i = 0; i < features.length; ++i)
                {
                    String word = features[i].split(":")[0];
                    Double val = Double.parseDouble(features[i].split(":")[1]);
                    tempfea.put(word,val);
                }
                trainfeature.add(tempfea);
            }
        }
    }


    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String linestr = value.toString();
        String filename = linestr.split("\t")[0];
        String[] features = linestr.split("\t")[1].split(" ");
        HashMap<String, Double> tempfea = new HashMap<>();

        for(int i = 0; i < features.length; ++i)
        {
            String word = features[i].split(":")[0];
            Double val = Double.parseDouble(features[i].split(":")[1]);
            tempfea.put(word,val);
        }


        double[] sim = new double[k];
        int[] indexoftrain = new int[k];

        for(int i = 0; i < k; ++i)
        {
            sim[i] = 10000;
            indexoftrain[i] = -1;
        }

        for (int i = 0; i < trainfeature.size(); i++) {
            double traintestsum = 0;
            double squaretrainsum = 0;
            double squaretestsum = 0;
            for(String word: trainfeature.get(i).keySet()){
                if(tempfea.keySet().contains(word)){
                    traintestsum += tempfea.get(word) * trainfeature.get(i).get(word);
                    squaretrainsum += trainfeature.get(i).get(word) * trainfeature.get(i).get(word);
                    squaretestsum += tempfea.get(word) * tempfea.get(word);
                }
                else{
                    squaretrainsum += trainfeature.get(i).get(word) * trainfeature.get(i).get(word);
                }
            }
            for(String word: tempfea.keySet()){
                if(trainfeature.get(i).keySet().contains(word) == false){
                    squaretestsum += tempfea.get(word)*tempfea.get(word);
                }
            }
            double similarity = traintestsum /(sqrt(squaretrainsum) * sqrt(squaretestsum));

            int temp = -1;
            for (int j = 0; j < k; j++) {
                if (abs(1-sim[j]) > abs(1-similarity)) {
                    if ((temp == -1) || (abs(1-sim[j]) > abs(1-sim[temp]))) {
                        temp = j;
                    }
                }
            }
            if (temp > -1) {
                sim[temp] = similarity;
                indexoftrain[temp] = i;
            }
        }


        ArrayList<Integer> classcount = new ArrayList<Integer>();
        for(int i = 0; i < allclass.size(); ++i)
            classcount.add(0);

        for(int i = 0; i < k; ++i)
        {
            int index = allclass.indexOf(trainclass.get((int) indexoftrain[i]));
            classcount.set(index,classcount.get(index)+1);
        }


        int max = classcount.get(0);
        int index = 0;

        for(int i = 1; i < classcount.size(); ++i)
        {
            if(classcount.get(i) > max)
            {
                max = classcount.get(i);
                index = i;
            }
        }

        context.write(new Text(filename), new Text(allclass.get(index)));
    }
}
