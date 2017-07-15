package com.careydevelopment.mapreducepatents;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZipPartitioner extends Partitioner <Text, IntWritable> {

	private static final Logger LOGGER = LoggerFactory.getLogger(ZipPartitioner.class);
	
	@Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {

		int partition = 0;
		String zip = key.toString();
		
		try {
			Integer zipInt = new Integer(zip);
			
			if (zipInt < 3333) partition = 1;
			else if (zipInt < 66666) partition = 2;
		} catch (Exception e) {
			LOGGER.warn("patent partitioner problem with " + zip);
		}
		
		return partition;
    }
}
