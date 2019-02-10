package st;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class SocialTriangle extends Configured implements Tool {

	private static final Logger logger = LogManager.getLogger(SocialTriangle.class);

	//Global counters
	public enum RESULT_COUNTER {
		finalCount,
		CountStatistics
	}

	public static class EdgesMapper extends Mapper<Object, Text, Text, Text> {

		int MAX = 50000;

		private Text outkey1 = new Text();
		private Text outvalue1 = new Text();
		private Text outkey2 = new Text();
		private Text outvalue2 = new Text();

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

			boolean flag1 = false;
			boolean flag2 = false;

			//split on comma, based on the input file format
			final StringTokenizer itr = new StringTokenizer(value.toString(),",");

			String temp1;
			String temp2;

			while (itr.hasMoreTokens()) {

				temp1 = itr.nextToken();

				if (Integer.parseInt(temp1) <= MAX) {

					flag1 = true;

					outkey2.set(temp1);

					//set the tag as "l" for indicating the LHS
					//This indicates userId2 followed by userId1
					outvalue1.set("l " + temp1);
				}

				temp2 = itr.nextToken();

				if (Integer.parseInt(temp2) <= MAX) {

					flag2 = true;

					outkey1.set(temp2);

					//set the tag as "r" meaning indicating the RHS
					//THis indicates userId1 follows userId2
					outvalue2.set("r " + temp2);
				}

				if (flag1 && flag2) {

					//on the LHS
					context.write(outkey1, outvalue1);

					//on the RHS
					context.write(outkey2, outvalue2);
				}
			}
		}
	}

	public static class PathLength2Mapper extends Mapper<Object, Text, Text, Text> {
		private Text outkey = new Text();
		private Text outvalue = new Text();

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

			//split on comma, based on the input file format
			final StringTokenizer itr = new StringTokenizer(value.toString(),",");

			String start;
			String end;

			while (itr.hasMoreTokens()) {

				start = itr.nextToken();
				itr.nextToken();
				end = itr.nextToken();

				//set flag as "a" to indicate data coming from this mapper
				outkey.set(end + "," + start);
				outvalue.set("a");

				context.write(outkey,outvalue);

			}
		}
	}

	public static class CloseTriangleMapper extends Mapper<Object, Text, Text, Text> {
		private Text outkey = new Text();
		private Text outvalue = new Text();

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

			//split on comma, based on the input file format
			final StringTokenizer itr = new StringTokenizer(value.toString(),",");

			String from;
			String to;

			while (itr.hasMoreTokens()) {

				from = itr.nextToken();
				to = itr.nextToken();

				//set flag as "b" to indicate data coming from this mapper
				outkey.set(from + "," + to);
				outvalue.set("b");

				context.write(outkey,outvalue);

			}
		}
	}

	public static class PathLength2Reducer extends Reducer<Text, Text, Text, Text> {

		private ArrayList<Text> FollowedByList = new ArrayList<>();
		private ArrayList<Text> FollowingList = new ArrayList<>();

		private Text Y = new Text();

		int counter1 = 0;

		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {

			// Clear our lists
			FollowedByList.clear();
			FollowingList.clear();

			//counter1 = 0;

			// iterate through all our values, binning each record based on what
			// it was tagged with
			// make sure to remove the tag!
			for (Text t : values) {

				if (t.charAt(0) == 'l') {
					FollowedByList.add(new Text(t.toString().substring(2)));
				} else if (t.charAt(0) == 'r') {
					FollowingList.add(new Text(t.toString().substring(2)));
				}
			}

			counter1 = FollowedByList.size() * FollowingList.size();
			context.getCounter(RESULT_COUNTER.CountStatistics).increment(counter1);

			// Execute our join logic now that the lists are filled
			executeJoinLogic(key, context);
		}

		private void executeJoinLogic(final Text key, Context context) throws IOException,
				InterruptedException {

			int counter = 0;

			// If both lists are not empty, join FollowedByList with FollowingList
			if (!FollowedByList.isEmpty() && !FollowingList.isEmpty()) {
				for (Text X : FollowedByList) {
					for (Text Z : FollowingList) {
						Y.set(key + "," + Z);
						context.write(X, Y);
						counter++;
					}
				}
			}
		}
	}

	public static class CLoseTriangleReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {

			int result;
			int ACounter = 0;
			int BCounter = 0;

			// iterate through all our values, binning each record based on what
			// it was tagged with
			// make sure to remove the tag!
			for (Text t : values) {

				if (t.charAt(0) == 'a') {

					//increment the counter for path-2 lengths
					ACounter++;
				} else if (t.charAt(0) == 'b') {

					//increment the counter for closing-edges
					BCounter++;
				}
			}

			//Number of path-2 length times the number of closing-edges will be our result
			result = ACounter * BCounter;

			//set the final count globally
			context.getCounter(RESULT_COUNTER.finalCount).increment(result);
		}
	}

	@Override
	public int run(final String[] args) throws Exception {

		boolean val;

		final Configuration conf1 = getConf();
		final Configuration conf2 = getConf();

		final Job job1 = Job.getInstance(conf1, "Social Triangle");
		final Job job2 = Job.getInstance(conf2, "Count Social Triangle");

		job1.setJarByClass(SocialTriangle.class);
		job2.setJarByClass(SocialTriangle.class);

		final Configuration jobConf1 = job1.getConfiguration();
		final Configuration jobConf2 = job2.getConfiguration();

		//job 1

		jobConf1.set("mapreduce.output.textoutputformat.separator", ",");

		//Set the sequential flow of job1
		job1.setMapperClass(EdgesMapper.class);
		job1.setReducerClass(PathLength2Reducer.class);

		//Set the output key and value types for job1
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		//Add input path to mapper in job1
		FileInputFormat.addInputPath(job1, new Path(args[0]));

		//Set the output path for job1
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		job1.waitForCompletion(true);

		//Get the count statistics using global counter
		Counters counters1 = job1.getCounters();
		Counter c1 = counters1.findCounter(RESULT_COUNTER.CountStatistics);

		logger.info("COUNT STATISTICS: " + c1.getValue());
		
		//job 2

		jobConf2.set("mapreduce.output.textoutputformat.separator", "");

		//Set the sequential flow of job2
		job2.setMapperClass(PathLength2Mapper.class);
		job2.setMapperClass(CloseTriangleMapper.class);
		job2.setReducerClass(CLoseTriangleReducer.class);

		//Set the output key and value types for job2
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		//Add input paths to each mapper in job2
		MultipleInputs.addInputPath(job2, new Path(args[1]),
				TextInputFormat.class, PathLength2Mapper.class);

		MultipleInputs.addInputPath(job2, new Path(args[0]),
				TextInputFormat.class, CloseTriangleMapper.class);

		//Set the output path for job2
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));

		val = job2.waitForCompletion(true);

		//output the total triangles using global counter
		Counters counters2 = job2.getCounters();
		Counter c2 = counters2.findCounter(RESULT_COUNTER.finalCount);

		//divide by three since (X,Y,Z),(Y,Z,X) and (Z,X,Y) will all be counted individually
		logger.info("FINAL COUNT: "+c2.getValue() / 3);

		return val ? 1 : 0;

	}

	public static void main(final String[] args) {
		if (args.length != 3) {
			throw new Error("Three arguments required:\n<input-dir> <output1-dir> <output2-dir>");
		}

		try {
			ToolRunner.run(new SocialTriangle(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}
