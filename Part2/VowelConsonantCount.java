package edu.rmit.cosc2367.s3823274.Part2;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * This mapper class maps emits pair of form (Vowel, 1) / (Consonant, 1) for
 * every word present in the input file
 */
public class VowelConsonantCount {

	private static final Logger LOG = Logger.getLogger(VowelConsonantCount.class);

	public static class VowelConsonantCountMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text wordType = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			// log that the mapper task has started
			LOG.setLevel(Level.INFO);
			LOG.info("The mapper task of Khushbu Patel, s3823274");

			// split the line from the input file into words
			StringTokenizer itr = new StringTokenizer(value.toString());

			// loop through all the words
			while (itr.hasMoreTokens()) {

				// convert the token to lower case and pass it to the method to identify if the
				// word starts with a vowel or a consonant
				String identifiedWordType = startsWithVowel(itr.nextToken().toLowerCase());

				// write the word in mapper output only if either starts with vowel or consonant
				if (!identifiedWordType.isEmpty()) {

					// set the word type as Vowel or Consonant
					wordType.set(identifiedWordType);

					// emit pair accordingly depending on whether the word starts with vowel or not
					context.write(wordType, one);
				}
			}
		}

		/**
		 * This method returns "Vowel" if the word starts with a vowel and return
		 * "Consonant" if it starts with a consonant, else return empty string
		 * 
		 * @param word
		 * @return
		 */
		public String startsWithVowel(String word) {

			// regex pattern to identify if the word starts with a consonant
			Pattern pattern = Pattern.compile("^[bcdfghjklmnpqrstvwxyz].*");
			Matcher matcher = pattern.matcher(word);

			// this is used to identify if the first character is a vowel or not
			char firstChar = word.charAt(0);
			
			if (firstChar == 'a' || firstChar == 'e' || firstChar == 'i' || firstChar == 'o' || firstChar == 'u') {
				return "Vowel";
			} else if (matcher.matches()) {
				return "Consonant";
			} else {
				return "";
			}
		}
	}

	/**
	 * This reducer class reduces the output and generates the total count of words
	 * that start with a vowel and a consonant
	 *
	 */
	public static class VowelConsonantCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			// log that the reducer task has started
			LOG.setLevel(Level.INFO);
			LOG.info("The reducer task of Khushbu Patel, s3823274");

			// loop through the values list to calculate the total count
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}

			// write the final output containing the count of words starting with Vowel and
			// Consonant
			result.set(sum);
			context.write(key, result);
		}
	}

	/**
	 * This is the main driver method for the VowelConsonantCount mapreduce program
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		// log that the task 2 has started
		LOG.setLevel(Level.INFO);
		LOG.info("Starting the task 2 for Khushbu Patel, s3823274");

		Configuration conf = new Configuration();

		// set configuration and create a job for VowelConsonantCount mapreduce program
		Job job = Job.getInstance(conf, "Vowel Consonant Count");
		job.setJarByClass(VowelConsonantCount.class);

		// set the mapper, combiner and reducer tasks respectively
		job.setMapperClass(VowelConsonantCountMapper.class);
		job.setCombinerClass(VowelConsonantCountReducer.class);
		job.setReducerClass(VowelConsonantCountReducer.class);

		// set output key class as Text to store the string containing the type of word
		job.setOutputKeyClass(Text.class);
		// set output value class as IntWritable to store the count of each word type
		job.setOutputValueClass(IntWritable.class);

		// delete output file path if already exists
		FileUtils.deleteDirectory(new File(args[1]));

		// set input and output file path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// exit the program once the job is finished
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
