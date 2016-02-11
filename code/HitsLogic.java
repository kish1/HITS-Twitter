import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Created by nikhilk on 11/30/15.
 */

public class HitsLogic {
    public final static String parent = "/Users/nikhilk/Documents/NEU_MSCS/MapReduce/Project/scores";
    public final static String links = "links.txt";
    public final static String part = "part-r-00000";


    public static class HubAuth{
        double prevs;
        double news;
        // boolean auth;

        public HubAuth(double prevs, double news){
            this.prevs = prevs;
            this.news = news;

        }
    }

    public static class HitsMap
            extends Mapper<Object, Text, Text, NullWritable> {
        private HashMap<Integer, HashSet<Integer>> out;
        private HashMap<Integer, HashSet<Integer>> in;
        private HashMap<Integer, HubAuth> hubScores;
        private HashMap<Integer, HubAuth> authScores;


        private Text mykey = new Text();

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // Initialize data structures
            hubScores = new HashMap<>();
            authScores = new HashMap<>();
            in = new HashMap<>();
            out = new HashMap<>();

            // Local variables for processing
            String line;
            Integer node;
            String direction;
            double prevs;
            double news;
            String str;
            HashSet<Integer> myset;



            // Read the MHubAuth File
            BufferedReader rdr = new BufferedReader(
                    new InputStreamReader(
                            new FileInputStream(
                                    new File(part))));

            // Populate the HashMap i.e. ID -> (Prev, New, Boolean of being Auth)
            while ((line = rdr.readLine()) != null) {
                StringTokenizer itr = new StringTokenizer(line);
                node = Integer.parseInt(itr.nextToken());
                str = itr.nextToken();
                prevs = Double.parseDouble(itr.nextToken());
                news = Double.parseDouble(itr.nextToken());

                if (str.equals("H")) {
                    hubScores.put(node, new HubAuth(prevs, news));
                } else {
                    authScores.put(node, new HubAuth(prevs, news));
                }

            }


            // Read the Links File
            BufferedReader link = new BufferedReader(
                    new InputStreamReader(
                            new FileInputStream(
                                    new File(links))));

            // Populate the HashMap i.e. ID -> Inlinks
            // Populate the HashMap i.e. ID -> Outlinks
            while ((line = link.readLine()) != null) {
                StringTokenizer itr = new StringTokenizer(line);
                node = Integer.parseInt(itr.nextToken());
                direction = itr.nextToken();

                myset = new HashSet<>();
                while (itr.hasMoreTokens()) {
                    myset.add(Integer.parseInt(itr.nextToken()));
                }
                if (direction.equals("O")) {
                    out.put(node, myset);
                } else if (direction.equals("I")) {
                    in.put(node, myset);
                }
            }

        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            HashSet<Integer> links;
            double news;
            double prevs;
            HubAuth current;
            String record;

            //For Auth then update auth score as sum of all HUB Scores
            for (int id : authScores.keySet()) {
                if (in.containsKey(id)) {
                    links = in.get(id);
                    news = 0;

                    for (int lid : links) {
                        current = hubScores.get(lid);
                        if (current != null) {
                            news = news + current.prevs;
                        }
                    }
                } else {
                    news = authScores.get(id).news;
                }
                prevs = authScores.get(id).prevs;

                // Now Flush the Auth Record
                record = id + " A " + prevs + " " + news;
                mykey.set(record);
                context.write(mykey, NullWritable.get());
            }


            //For HUB update HUB score as sum of all AUTH Scores
            for (int id : hubScores.keySet()) {
                if (out.containsKey(id)) {
                    links = out.get(id);
                    news = 0;

                    for (int lid : links) {
                        current = authScores.get(lid);
                        if (current != null) {
                            news = news + current.prevs;
                        }
                    }
                } else {
                    news = hubScores.get(id).news;
                }
                prevs = hubScores.get(id).prevs;

                // Now Flush the Auth Record
                record = id + " H " + prevs + " " + news;
                mykey.set(record);
                context.write(mykey, NullWritable.get());
            }
        }
    }



    public static class HitsReduce extends Reducer<Text, NullWritable, Text, NullWritable> {
        private HashMap<Integer, HubAuth> hubScores;
        private HashMap<Integer, HubAuth> authScores;
        Integer node;
        double prevs;
        double news;
        String str;

        // Variable to hold auth and hub factor
        double authFactor;
        double hubFactor;

        private Text mykey = new Text();

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException{
            hubScores = new HashMap<>();
            authScores = new HashMap<>();

            authFactor = 0;
            hubFactor = 0;
        }


        public void reduce(Text key, Iterable<NullWritable> values,
                           Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(key.toString());
            node = Integer.parseInt(itr.nextToken());
            str = itr.nextToken();
            prevs = Double.parseDouble(itr.nextToken());
            news = Double.parseDouble(itr.nextToken());

            if (str.equals("H")) {
                hubScores.put(node, new HubAuth(prevs, news));
                hubFactor = hubFactor + news * news;
            } else {
                authScores.put(node, new HubAuth(prevs, news));
                authFactor = authFactor + news * news;
            }

        }


        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            authFactor = Math.pow(authFactor, .5);
            hubFactor = Math.pow(hubFactor, .5);

            double news;
            double prevs;
            String record;

            for(int id : authScores.keySet()){
                news = authScores.get(id).news / authFactor;
                prevs = authScores.get(id).prevs;
                record = id + " A " + prevs + " " + news;
                mykey.set(record);
                context.write(mykey, NullWritable.get());
            }

            for(int id : hubScores.keySet()){
                news = hubScores.get(id).news / hubFactor;
                prevs = hubScores.get(id).prevs;
                record = id + " H " + prevs + " " + news;
                mykey.set(record);
                context.write(mykey, NullWritable.get());
            }

        }
    }

    public static String[] updateArgs(String [] args, int count){
        String outDir = parent + (count + 1) + "/" ;
        String inDir = parent + count + "/";
        String inputFile = inDir + part;

        // Get the Generic parser options
        String[] genOp = args[1].split(",");


        String myString = inputFile;

        // Generate Generic parser options
        for (int itr =1; itr < genOp.length; itr++){
            myString = myString + "," + genOp[itr];
        }

        // Update the output, input directory and Generic options
        args[args.length - 1 ] = outDir;
        args[args.length - 2] = inDir;
        args[1] = myString;
        return args;
    }


    public static void main(String[] args) throws Exception {
        int counter = 1;
        int max = 3;
        while(counter < max) {
            Configuration conf = new Configuration();

            GenericOptionsParser gop = new GenericOptionsParser(conf, args);
            String[] otherArgs = gop.getRemainingArgs();


            if (otherArgs.length < 2) {
                System.err.println("Usage: wordcount <in> [<in>...] <out>");
                System.exit(2);
            }
            Job job = new Job(conf, "word count");
            job.setJarByClass(HitsLogic.class);
            job.setMapperClass(HitsMap.class);
            job.setReducerClass(HitsReduce.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);

            job.setNumReduceTasks(1);
            for (int i = 0; i < otherArgs.length - 1; ++i) {
                FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
            }
            FileOutputFormat.setOutputPath(job,
                    new Path(otherArgs[otherArgs.length - 1]));

            // Run the job
            boolean complete = job.waitForCompletion(true);
            // Set the output path and Score file for next iteration.
            //System.out.println(Arrays.asList(args));
            args = updateArgs(args, counter);
            counter++;
        }
        System.exit(0);
    }

}
