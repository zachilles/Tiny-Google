
//package org.myorg;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Indexing {

	public static class Map1 extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			FileSplit fileSplit = (FileSplit) reporter.getInputSplit();
			String filename = fileSplit.getPath().toString();
			//String filename = fileSplit.getPath().getName();

			String line = value.toString().toLowerCase();
			String word = "";
			StringBuffer sb = new StringBuffer();
			for (char c : line.toCharArray()) {
				if (c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9') {
					sb.append(c);
				} else {
					word = sb.toString();
					if (word.length() > 0) {

						output.collect(new Text(filename + "##" + word), one);
					}
					sb = new StringBuffer();
				}

			}
			word = sb.toString();
			if (word.length() > 0) {
				output.collect(new Text(filename + "##" + word), one);
			}

		}
	}

	public static class Reduce1 extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	public static class Map2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		// private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String[] kv = value.toString().split("\t");
			String[] tmp = kv[0].split("##");
			output.collect(new Text(tmp[1]), new Text(kv[1] + "\t" + tmp[0]));

			// String[] kv = value.toString().split("##");
			// output.collect(new Text(kv[1]), new Text(value.toString() + "\t"
			// + kv[0]));

		}
	}

	public static class Reduce2 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		private InvertedIndex ii = new InvertedIndex();

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			while (values.hasNext()) {
				Text val = values.next();
				String tmp[] = val.toString().split("\t");
				IIItem item = new IIItem(tmp[1], Integer.parseInt(tmp[0]));
				ii.put(key.toString(), item);
			}
			// for (Text val : values) {
			//
			//
			// }
			output.collect(key, new Text(ii.output(key.toString())));
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Indexing.class);
		conf.setJobName("step1");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map1.class);
		conf.setCombinerClass(Reduce1.class);
		conf.setReducerClass(Reduce1.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);

		JobConf conf2 = new JobConf(Indexing.class);
		conf2.setJobName("step2");

		conf2.setOutputKeyClass(Text.class);
		conf2.setOutputValueClass(Text.class);

		conf2.setMapperClass(Map2.class);
		conf2.setReducerClass(Reduce2.class);
		conf2.setInputFormat(TextInputFormat.class);
		conf2.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf2, new Path(args[1]));
		FileOutputFormat.setOutputPath(conf2, new Path(args[2]));
		JobClient.runJob(conf2);

	}

}