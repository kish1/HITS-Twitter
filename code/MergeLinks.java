//package neu.mr.project;

/**
 * Created by nikhilk on 12/6/15.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


import java.io.*;
import java.util.StringTokenizer;


public class MergeLinks {

    public static class MapOut
            extends Mapper<Object, Text, Text, Text> {
        private Text mykey = new Text();
        private Text myvalue = new Text();
        private String mystring;


        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            // Initialize and get node
            StringTokenizer itr = new StringTokenizer(value.toString());
            String node = itr.nextToken();
            node = node + " O";

            // String builder for all the Adjacency list links
            StringBuilder strBuilder = new StringBuilder();
                while (itr.hasMoreTokens()) {
                    mystring = itr.nextToken();
                    strBuilder.append(mystring).append(" ");
                }

            mykey.set(node);
            myvalue.set(strBuilder.toString());
            context.write(mykey, myvalue);
        }
    }

    public static class MapIn
            extends Mapper<Object, Text, Text, Text> {
        private Text mykey = new Text();
        private Text myvalue = new Text();
        private String mystring;


        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            // Initialize and get node
            StringTokenizer itr = new StringTokenizer(value.toString());
            String node = itr.nextToken();
            node = node + " I";

            // String builder for all the Adjacency list links
            StringBuilder strBuilder = new StringBuilder();
            while (itr.hasMoreTokens()) {
                mystring = itr.nextToken();
                strBuilder.append(mystring).append(" ");
            }

            mykey.set(node);
            myvalue.set(strBuilder.toString());
            context.write(mykey, myvalue);
        }
    }

    public static class MAReduce extends Reducer<Text, Iterable<Text>, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            context.write(key, values.iterator().next());
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: MergeLinks <in> <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "MergeLinks");
        job.setJarByClass(MergeLinks.class);

        MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, MapOut.class);
        MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, MapIn.class);
        job.setReducerClass(MAReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}