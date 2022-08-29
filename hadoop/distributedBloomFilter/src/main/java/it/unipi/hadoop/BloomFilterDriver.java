package it.unipi.hadoop;

import it.unipi.hadoop.stage.BloomFilterGeneration;
import it.unipi.hadoop.stage.BloomFilterValidation;
import it.unipi.hadoop.stage.ParameterCalculation;
import it.unipi.hadoop.utility.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.Reader;

public class BloomFilterDriver {
    public static int[] percentageFalsePositive(Configuration conf, String pathString) {
        int[] result = new int[10];

        try {
            Path pt = new Path(pathString);
            Reader reader = new Reader(conf, Reader.file(pt));

            IntWritable key = new IntWritable();
            IntWritable value = new IntWritable();
            while (reader.next(key, value)) {
                int index = key.get() - 1;
                result[index] = value.get();
                key = new IntWritable();
                value = new IntWritable();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }

    public static void main(String[] args) throws Exception {
        String[] param1 = {"0.01", "data.tsv", "parameter"};
        long start = System.currentTimeMillis(); // start first timer for Stage 1
        if (!ParameterCalculation.main(param1)) {
            System.err.println("Stage 1 failed");
            return;
        }
        long end = System.currentTimeMillis(); // end first timer for Stage 1
        float sec = (end - start) / 1000F;
        Log.writeLog("stage-duration.txt", Float.toString(sec));
        System.out.println("- Stage 1 duration -> " + sec + " seconds"); // print Stage 1 duration

        String[] param2 = {"data.tsv", "filter"};
        start = System.currentTimeMillis(); // start second timer for Stage 2
        if (!BloomFilterGeneration.main(param2)) {
            System.err.println("Stage 2 failed");
            return;
        }
        end = System.currentTimeMillis(); // stop second timer for Stage 2
        sec = (end - start) / 1000F;
        Log.writeLog("stage-duration.txt", Float.toString(sec));
        System.out.println("- Stage 2 duration -> " + sec + " seconds"); // print Stage 2 duration

        String[] param3 = {"data.tsv", "falsePositive"};
        start = System.currentTimeMillis(); // start third timer for Stage 3
        if (!BloomFilterValidation.main(param3)) {
            System.err.println("Stage 3 failed");
            return;
        }
        end = System.currentTimeMillis(); // stop third timer for Stage 3
        sec = (end - start) / 1000F;
        Log.writeLog("stage-duration.txt", Float.toString(sec));
        System.out.println("- Stage 3 duration -> " + sec + " seconds"); // print Stage 3 duration

        String path = "hdfs://hadoop-namenode:9820/user/hadoop/falsePositive/part-r-00000";
        int[] falsePositive = percentageFalsePositive(new Configuration(), path);
        for (int i = 0; i < falsePositive.length; i++)
            System.out.println("Rating: " + (i + 1) + " False Positive Count : " + falsePositive[i]);
    }
}
