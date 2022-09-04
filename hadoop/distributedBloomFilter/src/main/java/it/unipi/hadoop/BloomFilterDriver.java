package it.unipi.hadoop;

import it.unipi.hadoop.stage.BloomFilterGeneration;
import it.unipi.hadoop.stage.BloomFilterValidation;
import it.unipi.hadoop.stage.ParameterCalculation;
import it.unipi.hadoop.utility.Log;
import it.unipi.hadoop.utility.ConfigManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.Reader;

import java.io.*;
import java.util.Iterator;

public class BloomFilterDriver {
    /*
    Calculate false positive rate
    input : configuration and path to the files to be read
    return : false positive rates
     */
    public static double[] percentageFalsePositive(Configuration conf, String pathString) throws IOException {
        int total_n = 0;
        int totalRating = 10;
        int[] result = new int[totalRating];
        Path ptStage1 = new Path(pathString + ConfigManager.getOutputStage1() + "/");
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] status = fs.listStatus(ptStage1);
        // Extraction of the "n" for each rating (output stage1)
        for (FileStatus fileStatus : status) {
            if (!fileStatus.getPath().toString().endsWith("_SUCCESS")) {
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fileStatus.getPath())));

                for (Iterator<String> it = br.lines().iterator(); it.hasNext(); ) {
                    String line = it.next();
                    String[] tokens = line.split("\t");
                    int rating = Integer.parseInt(tokens[0]);
                    int n = Integer.parseInt(tokens[1]);
                    int m = Integer.parseInt(tokens[2]);
                    int k = Integer.parseInt(tokens[3]);
                    result[(rating - 1)] = n;
                    total_n += n;
                }
                br.close();
                //fs.close();
            }
        }
        // Extraction of false positives for each rating (output stage3)
        int[] result2 = new int[totalRating];
        Path pt = new Path(pathString + ConfigManager.getOutputStage3() + "/");
        FileSystem fs1 = FileSystem.get(conf);
        FileStatus[] status1 = fs1.listStatus(pt);
        for (FileStatus fileStatus : status1) {
            if (!fileStatus.getPath().toString().endsWith("_SUCCESS")) {

                try {
                    Reader reader = new Reader(conf, Reader.file(new Path(fileStatus.getPath().toString())));

                    IntWritable key = new IntWritable();
                    IntWritable value = new IntWritable();
                    while (reader.next(key, value)) {
                        int index = key.get() - 1;
                        result2[index] = value.get();
                        key = new IntWritable();
                        value = new IntWritable();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        //Percentage calculation of false positives with respect to the total of n
        double[] percentage = new double[totalRating];
        for (int i = 0; i < totalRating; i++) {
            // System.out.println(i + " | n = " + result[i] + " | fp = " + result2[i]);
            percentage[i] = (double) result2[i] / (double) (total_n - result[i]);
        }

        return percentage;
    }

    public static void main(String[] args) throws Exception {
        // load config file
        ConfigManager.importConfig("config.json");

        String[] param1 = {ConfigManager.getFalsePositiveRate() + "", ConfigManager.getInput(), ConfigManager.getOutputStage1()};
        long start = System.currentTimeMillis(); // start first timer for Stage 1
        if (!ParameterCalculation.main(param1)) {
            System.err.println("Stage 1 failed");
            return;
        }
        long end = System.currentTimeMillis(); // end first timer for Stage 1
        float sec = (end - start) / 1000F;
        Log.writeLocal(ConfigManager.getStatsFile(), Float.toString(sec));
        System.out.println("- Stage 1 duration -> " + sec + " seconds"); // print Stage 1 duration

        String[] param2 = {ConfigManager.getInput(), ConfigManager.getOutputStage2()};
        start = System.currentTimeMillis(); // start second timer for Stage 2
        if (!BloomFilterGeneration.main(param2)) {
            System.err.println("Stage 2 failed");
            return;
        }
        end = System.currentTimeMillis(); // stop second timer for Stage 2
        sec = (end - start) / 1000F;
        Log.writeLocal(ConfigManager.getStatsFile(), Float.toString(sec));
        System.out.println("- Stage 2 duration -> " + sec + " seconds"); // print Stage 2 duration

        String[] param3 = {ConfigManager.getInput(), ConfigManager.getOutputStage3()};
        start = System.currentTimeMillis(); // start third timer for Stage 3
        if (!BloomFilterValidation.main(param3)) {
            System.err.println("Stage 3 failed");
            return;
        }
        end = System.currentTimeMillis(); // stop third timer for Stage 3
        sec = (end - start) / 1000F;
        Log.writeLocal(ConfigManager.getStatsFile(), Float.toString(sec));
        System.out.println("- Stage 3 duration -> " + sec + " seconds"); // print Stage 3 duration

        Log.writeLocal(ConfigManager.getStatsFile(), "------ end execution ------");

        String path = ConfigManager.getRoot();
        double[] falsePositive = percentageFalsePositive(new Configuration(), path);
        for (int i = 0; i < falsePositive.length; i++)
            System.out.println("Rating: " + (i + 1) + " False Positive Count : " + falsePositive[i]);

        // write results on output file
        try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(ConfigManager.getOutputFile()), "utf-8"))) {
            for (int i = 0; i < falsePositive.length; i++)
                writer.write((i + 1) + "," + falsePositive[i] + "\n");
        }
    }
}
