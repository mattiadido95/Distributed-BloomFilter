package it.unipi.hadoop.stage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import it.unipi.hadoop.model.BloomFilter;
import it.unipi.hadoop.utility.Log;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class BloomFilterGeneration {

    private static int[] readFilterParameter(Configuration conf, String pathString) throws IOException {
        Path pt = new Path(pathString);// Location of file in HDFS
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] status = fs.listStatus(pt);
        int[] result = new int[30];
        for (FileStatus fileStatus : status) {
            if (!fileStatus.getPath().toString().endsWith("_SUCCESS")) {
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fileStatus.getPath())));

                for(Iterator<String> it = br.lines().iterator(); it.hasNext(); ) {
                    String line = it.next();
                    String[] tokens = line.split("\t");
                    int rating = Integer.parseInt(tokens[0]);
                    int n = Integer.parseInt(tokens[1]);
                    int m = Integer.parseInt(tokens[2]);
                    int k = Integer.parseInt(tokens[3]);
                    result[(rating-1)*3] = n;
                    result[(rating-1)*3+1] = m;
                    result[(rating-1)*3+2] = k;
                }
                br.close();
                fs.close();
            }
        }
        return result;
    }

    public static class BloomFilterGenerationMapper extends Mapper<Object, Text, IntWritable, BloomFilter> {
        private BloomFilter[] bf;
        private final int maxRating = 10;

        @Override
        protected void setup(Context context) {

            this.bf = new BloomFilter[maxRating];

            for(int i=0; i<maxRating; i++){
                int index = i+1;
                int m = context.getConfiguration().getInt("filter." + index + ".parameter.m",0);
                int k = context.getConfiguration().getInt("filter." + index + ".parameter.k",0);
                //Log.writeLog("Setup : " + m + "\t" + k);
                //no film for rating i
                if(m == 0 || k == 0)
                    bf[i] = null;
                else
                    bf[i] = new BloomFilter(m,k);
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws NumberFormatException {
            String record = value.toString();
            if (record == null || record.length() == 0)
                return;

            String[] tokens = record.split("\t");

            if (tokens[0].equals("tconst"))
                return;

            // <title, rating, numVotes>
            if (tokens.length == 3) {
                int roundedRating = (int) Math.round(Double.parseDouble(tokens[1]));
                if(bf[roundedRating-1] != null) {
                    bf[roundedRating - 1].add(tokens[0]);
                    //Log.writeLog(context.getConfiguration(), "Map : " + tokens + "\t" + bf[roundedRating - 1]);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (int i = 0; i < maxRating; i++)
                if (bf[i] != null) {
                    //Log.writeLog(context.getConfiguration(),"Cleanup : " + bf[i].toString());
                    context.write(new IntWritable(i + 1), bf[i]);
            }
        }
    }

    public static class BloomFilterGenerationReducer extends Reducer<IntWritable, BloomFilter, IntWritable, BloomFilter> {

        @Override
        public void reduce(IntWritable key, Iterable<BloomFilter> values, Context context) throws IOException, InterruptedException {
            BloomFilter bfTot;
            List<BloomFilter> bfs = new ArrayList<>();

            for(BloomFilter bf : values)
                bfs.add(bf);

            int m = context.getConfiguration().getInt("filter." + (key.get()) + ".parameter.m",0);
            int k = context.getConfiguration().getInt("filter." + (key.get()) + ".parameter.k",0);
            //Log.writeLog("Reducer : " + m + "\t" + k);
            bfTot = new BloomFilter(m,k,bfs);
            context.write(key, bfTot);
        }

    }

    public static boolean main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: BloomFilterGeneration <input> <output>");
            System.exit(1);
        }
        System.out.println("args[0]: <input>=" + otherArgs[0]);
        System.out.println("args[1]: <output>=" + otherArgs[1]);

        Job job = Job.getInstance(conf, "BloomFilterGeneration");
        job.setJarByClass(BloomFilterGeneration.class);
        // set mapper/reducer
        job.setMapperClass(BloomFilterGenerationMapper.class);
        job.setReducerClass(BloomFilterGenerationReducer.class);

        // define mapper's output key-value
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(BloomFilter.class);

        // define reducer's output key-value
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(BloomFilter.class);

//        //DEBUG
//        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 1244453);
//        job.setNumReduceTasks(1);

        try {
            int[] result = readFilterParameter(conf, "hdfs://hadoop-namenode:9820/user/hadoop/parameter/");
            for(int i = 0; i < 10; i++) {
                int index = ((i)*3);
                job.getConfiguration().setInt("filter." + (i+1) + ".parameter.n", result[index]);
                job.getConfiguration().setInt("filter." + (i+1) + ".parameter.m", result[index+1]);
                job.getConfiguration().setInt("filter." + (i+1) + ".parameter.k", result[index+2]);
            }
        }catch(IOException ioe){
            ioe.printStackTrace();
        }

        // define I/O
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        return job.waitForCompletion(true);
    }
}