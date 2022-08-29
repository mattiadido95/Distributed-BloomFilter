package it.unipi.hadoop.stage;

import java.io.IOException;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ParameterCalculation {

    /*
    input: title, rating
    output: rating, n (number of films for that rating)
     */
    public static class ParameterCalculationMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        private int[] counter;
        private final int maxRating = 10;

        @Override
        protected void setup(Context context){
            counter = new int[maxRating];
        }

        @Override
        public void map(Object key, Text value, Context context) throws NumberFormatException {
            String record = value.toString();
            if (record == null || record.length() == 0)
                return;

            String[] tokens = record.split("\t");

            //skip file header
            if(tokens[0].equals("tconst"))
                return;

            // <title, rating, numVotes>
            if (tokens.length == 3) {
                int roundedRating = (int) Math.round(Double.parseDouble(tokens[1]));
                this.counter[roundedRating-1]++;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (int i=0; i < maxRating; i++)
                //emit only if there are films of rating i
                if(counter[i] > 0)
                    context.write( new IntWritable(i+1), new IntWritable(counter[i]) );
        }
    }

    /*
    input: rating, n (number of films for that rating)
    output: rating, (n,m,k) (parameter of the bloom filter)
     */
    public static class ParameterCalculationReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int n = 0;
            int m,k;
            double p = context.getConfiguration().getDouble("parameter.calculation.p",0.05);

            // merge mapper's counters for rating = key
            for (IntWritable value : values)
                n += value.get();

            m = (int) (- ( n * Math.log(p) ) / (Math.pow(Math.log(2),2.0)));
            k = (int) ((m/n) * Math.log(2));
            Text value = new Text(n + "\t" + m + "\t" + k);
            context.write(key, value);
        }

    }

    public static boolean main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: ParameterCalculation <p> <input> <output>");
            System.exit(1);
        }
        System.out.println("args[0]: <p>=" + otherArgs[0]);
        System.out.println("args[1]: <input>=" + otherArgs[1]);
        System.out.println("args[2]: <output>=" + otherArgs[2]);

        Job job = Job.getInstance(conf, "ParameterCalculation");
        job.setJarByClass(ParameterCalculation.class);

        // set mapper/reducer
        job.setMapperClass(ParameterCalculationMapper.class);
        job.setReducerClass(ParameterCalculationReducer.class);

        // define mapper's output key-value
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        // define reducer's output key-value
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // set p for parameter calculation
        Double p = Double.parseDouble(otherArgs[0]);
        job.getConfiguration().setDouble("parameter.calculation.p", p);

        // define I/O
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true);
    }
}
