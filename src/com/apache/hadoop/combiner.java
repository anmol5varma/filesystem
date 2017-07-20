package com.apache.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class combiner extends Reducer<IntWritable,Text,IntWritable,Text>{
	//private valuemeantuple result=new valuemeantuple();
	//private minima two = new minima();
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException,InterruptedException
	{
		for(Text v:values)
		context.write(key, new Text(v));
		//System.out.println(key.toString()+"hello");
	}
}