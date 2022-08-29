package it.unipi.hadoop;

import it.unipi.hadoop.stage.BloomFilterGeneration;
import it.unipi.hadoop.stage.BloomFilterValidation;
import it.unipi.hadoop.stage.ParameterCalculation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.Reader;

public class BloomFilterDriver
{
    public static int[] percentageFalsePositive(Configuration conf, String pathString) {
        int[] result = new int[10];

        try {
            Path pt = new Path(pathString);
            Reader reader = new Reader(conf, Reader.file(pt));

            IntWritable key = new IntWritable();
            IntWritable value = new IntWritable();
            while(reader.next(key, value)) {
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

    public static void main( String[] args ) throws Exception {
        String[] param1 = {"0.01", "data.tsv", "parameter"};
        if (!ParameterCalculation.main(param1)) {
            System.err.println("Stage 1 failed");
            return;
        }

        String[] param2 = {"data.tsv", "filter"};
        if (!BloomFilterGeneration.main(param2)) {
            System.err.println("Stage 2 failed");
            return;
        }

        String[] param3 = {"data.tsv", "falsePositive"};
        if (!BloomFilterValidation.main(param3)) {
            System.err.println("Stage 3 failed");
            return;
        }

        String path = "hdfs://hadoop-namenode:9820/user/hadoop/falsePositive/part-r-00000";
        int[] falsePositive = percentageFalsePositive(new Configuration(), path);
        for (int i = 0; i < falsePositive.length; i++)
            System.out.println("Rating: " + (i + 1) + " False Positive Count : " + falsePositive[i]);
    }
}
