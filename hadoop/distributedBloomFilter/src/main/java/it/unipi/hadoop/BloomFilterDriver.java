package it.unipi.hadoop;

import it.unipi.hadoop.stage.BloomFilterGeneration;
import it.unipi.hadoop.stage.BloomFilterValidation;
import it.unipi.hadoop.stage.ParameterCalculation;
import it.unipi.hadoop.utility.ConfigManager;
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
        // load config file
        ConfigManager.importConfig("config.json");

        String[] param1 = {ConfigManager.getFalsePositiveRate() + "", ConfigManager.getInput(), ConfigManager.getOutputStage1()};
        if (!ParameterCalculation.main(param1)) {
            System.err.println("Stage 1 failed");
            return;
        }

        String[] param2 = {ConfigManager.getInput(), ConfigManager.getOutputStage2()};
        if (!BloomFilterGeneration.main(param2)) {
            System.err.println("Stage 2 failed");
            return;
        }

        String[] param3 = {ConfigManager.getInput(), ConfigManager.getOutputStage3()};
        if (!BloomFilterValidation.main(param3)) {
            System.err.println("Stage 3 failed");
            return;
        }

        String path = ConfigManager.getRoot() + ConfigManager.getOutputStage3() + "/part-r-00000";
        int[] falsePositive = percentageFalsePositive(new Configuration(), path);
        for (int i = 0; i < falsePositive.length; i++)
            System.out.println("Rating: " + (i + 1) + " False Positive Count : " + falsePositive[i]);
    }
}
