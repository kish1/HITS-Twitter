//package neu.mr.project;

/**
 * Created by nikhilk on 11/26/15.
 */


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.omg.PortableInterceptor.INACTIVE;


import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

public class LinkSort {

    public static class InlinksMapper
            extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

        private LongWritable mykey = new LongWritable();
        private LongWritable myval = new LongWritable();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(value.toString());
            String node = st.nextToken();
            String link;
            int count = 0;
            while(st.hasMoreTokens())
            {
            	link = st.nextToken();
            	count++;
            }

            mykey.set(count);
            myval.set(Integer.parseInt(node));
            context.write(mykey, myval);
        }
    }

    public static class InLinksReducer
            extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable>{

        public void reduce(LongWritable key, Iterable<LongWritable> values,
                          Context context) throws IOException, InterruptedException {

            for (LongWritable val : values) {;

                context.write(val, key);
            }

        }
    }

    /**
     * This comparator is used to sort the output of the text in the
     * descending order of the counts.
     */
    public static class SortKeyComparator extends WritableComparator {


        protected SortKeyComparator() {
            super(LongWritable.class, true);
        }

        /**
         * Compares in the descending order of the keys.
         */
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            LongWritable o1 = (LongWritable) a;
            LongWritable o2 = (LongWritable) b;
            if(o1.get() < o2.get()) {
                return 1;
            }else if(o1.get() > o2.get()) {
                return -1;
            }else {
                return 0;
            }
        }

    }

    public static class RangePartitioner extends Partitioner<LongWritable, LongWritable>{
        //int[] quantiles = {1, 3, 4, 5, 6, 7, 9, 14, 23, 49};
        int[] quantiles = {1, 2, 3, 4, 5, 6, 7, 9, 14};
    /**
     * Assumption the range values are + 1 to num partitions
    *  Num partitions or quantitles
     * **/
    public int bucketSearch(int value, int[] target){
        if (value > target[target.length - 1]){
            return target.length;
        } else if (value < target[0]) {
            return 0;
        }

        // Otherwise when value is between the range or equal to corners
        int left = 0;
        int right = target.length - 1;
        while (left <= right){
            int mid = (left + right) / 2;
            //System.out.println(target[mid]);
            if (value == target[mid]) {
                return mid;
            }
            else if(value < target[mid]){
                if (value > target[mid - 1]) {return mid;}
                else {right = mid - 1;}
            }
            else if(value > target[mid]) {
                if (value < target[mid + 1]) {return mid + 1;}
                else {left = mid + 1;}
            }
        }
        // Return just the second bucket value.
        return 1;
    }

        @Override
        public int getPartition(LongWritable key, LongWritable value, int numPartitions) {
            return bucketSearch((int) key.get(), quantiles);
        }

    }



    public static void main(String[] args) throws Exception {
        int numReduceTasks = 10;
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: InlinkSort <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "InlinkSort");

        job.setNumReduceTasks(numReduceTasks);
        job.setPartitionerClass(RangePartitioner.class);

        job.setJarByClass(LinkSort.class);
        job.setMapperClass(InlinksMapper.class);
        job.setSortComparatorClass(SortKeyComparator.class);


        job.setReducerClass(InLinksReducer.class);
        //job.setInputFormatClass(TextInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        //job.setOutputFormatClass(SequenceFileOutputFormat.class);
        //SequenceFileOutputFormat.setCompressOutput(job, true);
        //SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        //SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));

        /**
         RandomSampler constructor input
         double freq, int numSamples, int maxSplitsSampled
         Need to tweak the frequency and number of samples as per the data distribution
         **/

        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }

}