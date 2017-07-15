package com.careydevelopment.mapreducepatents;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PatentsByZip {

	private static final Logger LOGGER = LoggerFactory.getLogger(PatentsByZip.class);
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "patents by zip");
	    job.setJarByClass(PatentsByZip.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(ZipSumReducer.class);
	    job.setReducerClass(ZipSumReducer.class);
	    job.setPartitionerClass(ZipPartitioner.class);
	    job.setNumReduceTasks(3);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(FloatWritable.class);
	    FileInputFormat.addInputPath(job, new Path("/mapreduce/patents/input"));
	    FileOutputFormat.setOutputPath(job, new Path("/mapreduce/patents/output"));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
