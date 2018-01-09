
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

public class Searching {


	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		private String keywords = "";
		
		@Override
		public void configure(JobConf job) {
			keywords = job.get("keywords").toLowerCase();
		}
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			Hashtable<String, Integer> wordList = new Hashtable<String, Integer>();
			String word = "";
			StringBuffer sb = new StringBuffer();
			for (char c : keywords.toLowerCase().toCharArray()) {
				if (c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9') {
					sb.append(c);
				} else {
					word = sb.toString();
					if (word.length() > 0) {
						wordList.put(word, 1);
						// output.collect(new Text(filename + "##" + word), one);
					}
					sb = new StringBuffer();
				}

			}
			word = sb.toString();
			if (word.length() > 0) {
				wordList.put(word, 1);
				// output.collect(new Text(filename + "##" + word), one);
			}
			
			
			
			String line = value.toString();
			String[] kv = line.split("\t");
			//output.collect(new Text("key"), new Text(kv[1]));
			if (wordList.containsKey(kv[0])) {
				output.collect(new Text("Result:"), new Text(kv[1]));
			}

		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		ArrayList<IIItem> list = new ArrayList<IIItem>();

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			while (values.hasNext()) {
				String val = values.next().toString();
				String[] items = val.split(",");
				for (int i = 0; i < items.length; i++) {
					String[] tmp = items[i].split(":");
					boolean flag = true;
					for (int j = 0; j < list.size(); j++) {
						IIItem item = list.get(j);
						if (item.getID().equals(tmp[0])) {
							item.setWordCount(item.getWordCount() + 1);
							item.setCount(item.getCount() + 1);
							flag = false;
						}
					}
					if (flag) {
						list.add(new IIItem(tmp[0], Integer.parseInt(tmp[1]), 1));
					}
					
				}
			}

			Collections.sort(list, new ItemComparator());
			String result = "\n";
			for (int i = 0; i < list.size(); i++) {
				IIItem item = list.get(i);
				result = result + item.getID() + "\t" + item.getWordCount() + "\t" +item.getCount() + "\n";
			}
			result = result.substring(0, result.length()-1);
			output.collect(key, new Text(result));
		}
	}

	public static void main(String[] args) throws Exception {

		

		JobConf conf = new JobConf(Indexing.class);
		conf.setJobName("Searching");
		conf.set("keywords", args[0]);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[1]));
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));
		JobClient.runJob(conf);
		//System.out.println("sadfsadfasdfsadfsadfsadfasdf");

	}

}