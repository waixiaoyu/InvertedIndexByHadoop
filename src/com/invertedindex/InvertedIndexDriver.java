package com.invertedindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.yyy.utils.HadoopUtils;

public class InvertedIndexDriver {

	private final static String HOST = "128.6.5.42";

	public static void main(String args[]) throws Exception {

		if (args.length < 2) {
			// throw new Exception("Usage: <input directory> <output
			// directory>");
			args = new String[3];
			args[0] = "hdfs://" + HOST + ":9000/booklist/*";
			args[1] = "hdfs://" + HOST + ":9000/invertedindex/";
			args[2] = "hdfs://" + HOST + ":9000/stopwords.txt";
		}

		Configuration conf = new Configuration();
		HadoopUtils.deleteOutputDirectory(conf, new Path(args[1]));
		conf.set("stopfilePath", args[2]);
		Job job = Job.getInstance(conf);
		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);
		// job.setNumReduceTasks(3);
		job.setJarByClass(InvertedIndexDriver.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		System.out.println(job.waitForCompletion(true) ? 0 : 1);
	}

}
