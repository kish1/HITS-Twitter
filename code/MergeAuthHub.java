//package neu.mr.project;

/**
 * Created by nikhilk on 12/4/15.
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



public class MergeAuthHub {

    public static class MapAuth
            extends Mapper<Object, Text, Text, NullWritable> {
        String ID;
        String mykey;
        private Text word = new Text();


        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
                ID = value.toString();
                mykey = ID + " A " + " 1 1";
                word.set(mykey);
                context.write(word, NullWritable.get());
            }
        }

    public static class MapHub
            extends Mapper<Object, Text, Text, NullWritable> {
        String ID;
        String mykey;
        private Text word = new Text();


        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            ID = value.toString();
            mykey = ID + " H " + " 1 1";
            word.set(mykey);
            context.write(word, NullWritable.get());
        }
    }

    public static class MAReduce extends Reducer<Text, NullWritable, Text, NullWritable> {
        LongWritable myvalue = new LongWritable();

        public void reduce(Text key, Iterable<NullWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: MergeAuthHub <in> <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "MergeAuthHub");
        job.setJarByClass(MergeAuthHub.class);

        MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, MapHub.class);
        MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, MapAuth.class);
        job.setReducerClass(MAReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(1);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

