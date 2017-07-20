package com.apache.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class reducer extends Reducer<IntWritable,Text,NullWritable,Text>{
	//private valuemeantuple result=new valuemeantuple();
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException,InterruptedException
	{
		org.apache.hadoop.conf.Configuration conf=context.getConfiguration();
		String arg=conf.get("arg");
		String[] a=arg.split(":");
		String p=a[0];
		for(Text value:values)
		{
			System.out.println(value.toString());
			if(p==null)
				context.write(null, new Text(value.toString()));
			else
			{
				String ar="";
				String[] v=value.toString().split(",");
				String[] p1=p.split(" ");
				for(int i=0;i<p1.length;i++)
				{
					ar+=v[Integer.parseInt(p1[i])-1]+",";
				}
				System.out.println("ar= "+ar);
				//ar=ar.substring(0,p1.length-1);
				context.write(null, new Text(ar));
			}
		}
		//System.out.println(key.toString()+"hello");
	}
}