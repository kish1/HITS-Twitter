//package neu.mr.project;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import java.util.HashSet;

/**
 * Created by nikhilk on 12/3/15.
 */
public class GreaterNOutLinks {

    public static class FilterMap
            extends Mapper<Object, Text, Text, NullWritable> {
        int NVal = 50;
        HashSet<String> myset;


        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] split = value.toString().split("\\s+");

            if(split.length >= NVal )
                context.write(value, NullWritable.get());
            }

    }

    public static class FilterReduce extends Reducer<Text, NullWritable, Text, NullWritable> {


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
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "word count");
        job.setJarByClass(GreaterNOutLinks.class);
        job.setMapperClass(FilterMap.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(FilterReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(10);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));


        //DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(),
        //job.getConfiguration());

        //DistributedCache.setLocalFiles(job.getConfiguration(), otherArgs[0]);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}