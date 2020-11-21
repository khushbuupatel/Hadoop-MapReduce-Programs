package edu.rmit.cosc2367.s3823274.Part4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.rmit.cosc2367.s3823274.Part3.WordCount;

public class WordLengthCount {

	private static final Logger LOG = Logger.getLogger(WordCount.class);

	/**
	 * This is the main driver method for the WordLengthCount mapreduce program with
	 * customer partitioner
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		// log that the mapreduce program for task 3 has started
		LOG.setLevel(Level.INFO);
		LOG.info("Starting the task 4 for Khushbu Patel, s3823274");

		Configuration conf = new Configuration();

		// set configuration and create a job for WordCount mapreduce program
		Job job = Job.getInstance(conf, "Word Length Count with customer partitioner");
		job.setJarByClass(WordLengthCount.class);

		// set mapper and reducer class
		job.setMapperClass(WordLengthMapper.class);
		job.setReducerClass(WordLengthReducer.class);

		// set reducer as 2 and customer partitioner class
		job.setNumReduceTasks(2);
		job.setPartitionerClass(WordLengthPartitioner.class);

		// set output key class as Text to store the string containing the type of word
		job.setOutputKeyClass(Text.class);
		// set output value class as IntWritable to store the count of each word type
		job.setOutputValueClass(IntWritable.class);

		// set input and output file path
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// exit from the mapreduce program once the job is finished
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}