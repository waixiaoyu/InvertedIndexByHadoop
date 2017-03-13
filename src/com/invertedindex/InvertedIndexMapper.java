package com.invertedindex;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

	Set<String> stopWords = new HashSet<String>();

	// Keep a list of stop words
	@Override
	protected void setup(Context context) throws IOException {

		Configuration conf = context.getConfiguration();
		String filePath = conf.get("stopfilePath");

		Path pt = new Path(filePath);// Location of file in HDFS
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
		String line;
		line = br.readLine();
		while (line != null) {
			stopWords.add(line.trim().toLowerCase());
			line = br.readLine();
		}
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// https://hadoop.apache.org/docs/r2.4.1/api/org/apache/hadoop/mapred/FileSplit.html
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		Text name = new Text(fileName);

		// fast & iterator don't have to load all data
		StringTokenizer tokenizor = new StringTokenizer(value.toString());
		while (tokenizor.hasMoreTokens()) {
			String curWord = tokenizor.nextToken().toString().toLowerCase();
			curWord = curWord.replaceAll("[^a-zA-Z]", "");
			// filter out the stop word
			if (!stopWords.contains(curWord))
				context.write(new Text(curWord), name);
		}
	}
}
