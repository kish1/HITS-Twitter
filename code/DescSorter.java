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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.lang.Math;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

public class DescSorter {
	

	  public static class SortComparator extends WritableComparator
	  {
		  public SortComparator()
		  {
			  super(IntWritable.class, true);
		  }
		  
		  public int compare(WritableComparable ck1, WritableComparable ck2)
		  {
			  return ((IntWritable) ck2).compareTo(((IntWritable) ck1));
		  }
	  }

  public static class AdjListMapper 
       extends Mapper<Object, Text, IntWritable, IntWritable>{
    
	    private IntWritable okey;
	    private IntWritable oval;
	    
	  protected void setup(Context context)
	  {
		  okey = new IntWritable();
		  oval = new IntWritable();
	  }
    
      
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	StringTokenizer st = new StringTokenizer(value.toString());
    	String node, listElem;
    	int count = 0;
    	node = st.nextToken();
    	while(st.hasMoreTokens())
    	{
    		listElem = st.nextToken();
    		count++;
    	}
    	okey.set(count);
    	oval.set(Integer.parseInt(node));
    	context.write(okey, oval);
    }
  }
  
    
  public static class Reducer 
       extends Reducer<CompositeKey,IntWritable,IntWritable,Text> {
	  HashMap<Integer, int[]> delays;
	  
	  protected void setup(Context context)
	  {
		  delays = new HashMap<Integer, int[]>();
	  }
	  
	  protected void cleanup(Context context) throws IOException, InterruptedException
	  {
		  StringBuilder sb;
		  IntWritable key = new IntWritable();
		  Text val = new Text();
		  int[] months;
		  for(Map.Entry<Integer, int[]> entry : delays.entrySet())
		  {
			  sb = new StringBuilder("");
			  key.set((Integer) entry.getKey());
			  months = (int[]) entry.getValue();
			  for(int i=0; i<months.length; i++)
			  {
				  sb.append("(" + Integer.toString(i+1) + "," + Integer.toString(months[i]) + ") ");
			  }
			  val.set(sb.toString());
			  context.write(key, val);
		  }
	  }
	  
    public void reduce(CompositeKey key, Iterable<IntWritable> values, 
                       Context context)  {
    	
    	int sum = 0, count = 0;
    	double avg;
    	
    	for(IntWritable iw : values) {
	    	sum += iw.get();
	    	count++;
    	}
    	avg = ((double) sum)/count;
    	avg = Math.round(avg);
    	
    	if(delays.containsKey(key.getAirlineID()))
    	{
    		(delays.get(key.getAirlineID()))[key.getMonth()-1] = (int) avg; 
    	}
    	else
    	{
    		int[] months = new int[12];
    		for(int i=0; i<months.length; i++)
    			months[i] = 0;
    		months[key.getMonth()-1] = (int) avg;
    		delays.put(key.getAirlineID(), months);
    	}
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: flights <in> <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "AvgDelays");
    job.setJarByClass(DescSorter.class);
    job.setMapperClass(FlightMapper.class);
    
    job.setMapOutputKeyClass(CompositeKey.class);
    job.setMapOutputValueClass(IntWritable.class);
   
    job.setPartitionerClass(CompositeKeyPartitioner.class);
    job.setSortComparatorClass(SortComparator.class);
    job.setGroupingComparatorClass(GroupingComparator.class);
    
    job.setReducerClass(AvgDelayReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
