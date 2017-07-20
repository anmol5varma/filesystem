package com.apache.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class mapper extends Mapper<LongWritable, Text, IntWritable, Text>{
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
	{
		System.out.println("*****************Inside mapper.java***************");
		String s=value.toString();
		//System.out.println(s);
		String str[]=s.split(",");
		org.apache.hadoop.conf.Configuration conf=context.getConfiguration();
		String arg=conf.get("arg");
		String[] a=arg.split(":");
		System.out.println(arg);
		String r="", e="";
		if(a.length ==3)
			{r=a[1]; e=a[2];}
		else if(a.length==2)
			{
			r=a[1];
			}
			
		int c=0;
		//for(String s1: str)
		//{
			if(c>0)
			{
				if(r=="" && e=="")
				{
					context.write(new IntWritable(c), new Text(s));
				}
				else if(r!="")
				{
					String[] r1=r.split(" ");
					if(str[Integer.parseInt(r1[0])-1].compareTo(str[Integer.parseInt(r1[1])-1])>=0&&str[Integer.parseInt(r1[0])-1].compareTo(str[Integer.parseInt(r1[2])-1])<=0)
						context.write(new IntWritable(c), new Text(s));
				}
				else if(e!=null)
				{
					String[] e1=e.split(" ");
					if(str[Integer.parseInt(e1[0])-1].compareTo(str[Integer.parseInt(e1[1])-1])==0)
						context.write(new IntWritable(c), new Text(s));
				}
			}
			else
			{
				context.write(new IntWritable(c), new Text(s));
			}
			//System.out.println(str[0]+":"+one.getmin().get()+" "+one.getmin().get());
			c++;
		//}
	}
}