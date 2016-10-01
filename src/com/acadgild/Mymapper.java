package com.acadgild;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mymapper extends Mapper<LongWritable,Text,LongWritable,Text>{

	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
		
		String line=value.toString();
		String[] st=line.split("\n");
		for(String record:st){
			if(record.contains("NA"))
				context.write(new LongWritable(), new Text(record));
		}
	}
}
