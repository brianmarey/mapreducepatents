package com.careydevelopment.mapreducepatents;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZipSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ZipSumReducer.class);

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		LOGGER.info("patent reduce " + key);
		
		int sum = 0;
    
	    for (IntWritable intWritable : values) {
	        int i = intWritable.get();
	        sum+=i;
	    }
    
	    LOGGER.info("patent reduce2 putting " + key + " " + sum);

	    context.write(key, new IntWritable(sum));
	}
}
