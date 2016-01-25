/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//package org.apache.hadoop.examples;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.zip.GZIPInputStream;

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

public class Authset {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, IntWritable, IntWritable>{
    
    HashSet<Integer> partial_hubs;
    IntWritable mkey = new IntWritable();
    IntWritable one = new IntWritable(1);
    
    
    protected void setup(Context context) throws IOException
    {
    	partial_hubs = new HashSet<Integer>();
    	/*
    	Path[] files = DistributedCache.getLocalCacheFiles(context
				.getConfiguration());

		if (files == null || files.length == 0) {
			throw new RuntimeException(
					"User information is not set in DistributedCache");
		}
		*/
		// Read all files in the DistributedCache
		//for (Path p : files) {
			BufferedReader rdr = new BufferedReader(
					new InputStreamReader(
							new FileInputStream(
									new File("partial_hubs.txt"))));
			String line;
			// For each record in the user file
			while ((line = rdr.readLine()) != null) {

				// Get the user ID for this record
				
				Integer node = Integer.parseInt(line.trim());
				partial_hubs.add(node);
			}
		//}

    }
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      Integer node = Integer.parseInt(itr.nextToken());
      if(partial_hubs.contains(node))
      {
    	  while (itr.hasMoreTokens()) {
    	        mkey.set(Integer.parseInt(itr.nextToken()));
    	        context.write(mkey, one);
    	      }  
      }
      
    }
  }
  
  public static class IntSumReducer 
       extends Reducer<IntWritable,IntWritable,IntWritable, NullWritable> {
    private IntWritable result = new IntWritable();
    private int min_threshold = 1500;

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    	int count = 0;
    	for(IntWritable i : values)
    		count++;
    	if(count >= min_threshold)
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
    job.setJarByClass(Authset.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(NullWritable.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(IntWritable.class);
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
