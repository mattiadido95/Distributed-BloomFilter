package it.unipi.hadoop.stage;

import it.unipi.hadoop.model.BloomFilter;
import it.unipi.hadoop.utility.ConfigManager;
import it.unipi.hadoop.utility.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;

public class BloomFilterValidation {
    private static BloomFilter[] readFilter(Configuration conf, String pathString) throws IOException {
        BloomFilter[] result = new BloomFilter[10];
        try {
            Path pt = new Path(pathString);
            Reader reader = new Reader(conf, Reader.file(pt));

            IntWritable key = new IntWritable();
            BloomFilter value = new BloomFilter();
            while(reader.next(key, value)) {
                int index = key.get() - 1;
                result[index] = value;
                key = new IntWritable();
                value = new BloomFilter();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }

    public static class BloomFilterValidationMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        private BloomFilter[] bf;
        private int[] counter;
        private final int maxRating = 10;

        @Override
        protected void setup(Context context) throws IOException {
            String path = ConfigManager.getRoot() + ConfigManager.getOutputStage2() + "/part-r-00000";
            this.bf = readFilter(context.getConfiguration(), path);
            counter = new int[maxRating];
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

                for (int i = 0; i < maxRating; i++) {
                    if (roundedRating == i+1)
                        continue;

                    if (bf[i].find(tokens[0]))
                        counter[i]++;
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (int i = 0; i < maxRating; i++) {
                if (counter[i] > 0) {
                    context.write(new IntWritable(i + 1), new IntWritable(counter[i]));
                }
            }
        }
    }

    public static class BloomFilterValidationReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int falsePositive = 0;

            for (IntWritable counter : values)
                falsePositive += counter.get();

            context.write(key, new IntWritable(falsePositive));
        }
    }

    public static boolean main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: BloomFilterValidation <input> <output>");
            System.exit(1);
        }
        System.out.println("args[0]: <input>=" + otherArgs[0]);
        System.out.println("args[1]: <output>=" + otherArgs[1]);

        Job job = Job.getInstance(conf, "BloomFilterValidation");
        job.setJarByClass(BloomFilterValidation.class);
        // set mapper/reducer
        job.setMapperClass(BloomFilterValidation.BloomFilterValidationMapper.class);
        job.setReducerClass(BloomFilterValidation.BloomFilterValidationReducer.class);

        // define mapper's output key-value
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        // define reducer's output key-value
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        // define I/O
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        return job.waitForCompletion(true);
    }
}
