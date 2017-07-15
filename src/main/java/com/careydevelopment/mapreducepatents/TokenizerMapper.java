package com.careydevelopment.mapreducepatents;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TokenizerMapper.class);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");
        String zip = getZip(parts);
        
        if (zip != null && zip.length() > 3) {
        	LOGGER.info("patent writing " + zip + " to 1" );
            context.write(new Text(zip), new IntWritable(1));            
        }
    }
    
    
    private String getZip(String[] parts) {
    	String zip = null;
    	
    	if (parts != null && parts.length == 11) {
    		zip = stripQuotes(parts[9]);
    	}
    	
    	if (zip != null) {
    		int dash = zip.indexOf("-");
    		if (dash > -1) {
    			zip = zip.substring(0, dash);
    		}
    	}
    	
    	return zip;
    }
    
    
    private String stripQuotes(String str) {
    	if (str.startsWith("\"")) {
    		str = str.substring(1, str.length());
    	}
    	
    	if (str.endsWith("\"")) {
    		str = str.substring(0, str.length() - 1);
    	}
    	
    	return str;
    }
}
