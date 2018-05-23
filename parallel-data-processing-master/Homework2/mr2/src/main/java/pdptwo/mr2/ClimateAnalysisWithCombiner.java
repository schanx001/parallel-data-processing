package pdptwo.mr2;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Class ClimateAnalysisWithCombiner
 * 
 * Uses combiner 
 * 
 * @author schanx
 *
 */
public class ClimateAnalysisWithCombiner {
	/**
	 * Mapper class
	 * Checks if records are of type TMAX and TMIN
	 * Mapper emits (stationId,temperatureRecord)
	 * 
	 * @author schanx
	 *
	 */
	public static class ClimateAnalysisMapper
	extends Mapper<Object, Text, Text, Text>{

		//private final static IntWritable one = new IntWritable(1);
		private String[] record;
		private Text stationId=new Text();
		private Text recordTemperature = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				record = itr.nextToken().split(",");
				if(record[2].equals("TMAX") || record[2].equals("TMIN")) {
					stationId.set(record[0]);
					recordTemperature.set(record[2]+","+record[3]);
					context.write(stationId,recordTemperature);
				}
			}
		}
	}

	/**
	 * Class ClimateAnalysisCombiner
	 * 
	 * extends Reducer
	 * reads output from map and emits output to be used by reducer
	 * outputs in the form of map's output  
	 * not guaranteed to run, dependent on the Hadoop enivironment
	 * 
	 * calculates the partial sums and counts for the reducer to work on further
	 * 
	 * @author schanx
	 *
	 */
	public static class ClimateAnalysisCombiner
	extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> values,
				Context context
				) throws IOException, InterruptedException {
			double tmaxSum = 0;
			double tminSum = 0;
			int tmaxCount = 0;
			int tminCount = 0;
			Text res1 = new Text();
			Text res2 = new Text();
			for (Text val : values) {
				String[] record = val.toString().split(",");
				if(record[0].equals("TMAX")) {
					tmaxSum+=Integer.parseInt(record[1]);
					tmaxCount+=1;
				}
				else {
					tminSum+=Integer.parseInt(record[1]);
					tminCount+=1;
				}
			}
			context.write(key,new Text("TMAX," +String.valueOf(tmaxSum)+","+String.valueOf(tmaxCount)));

			System.out.println(tmaxSum/tmaxCount);
			System.out.println(tminSum/tminCount);
			//res.set("TMAX" +String.valueOf(tmaxSum/tmaxCount));
			//res.set("TMIN" +String.valueOf(tminSum/tminCount));
			context.write(key,new Text("TMIN," +String.valueOf(tminSum)+","+String.valueOf(tminCount)));
		}
	}

	/**
	 * ClimateAnalysisReducer class
	 * 
	 * reduces the output from combiner or directly from mapper if the combiner doesn't run
	 * calculates total sum and count and averages for the records per station
	 * writes the final output to a file
	 * @author schanx
	 *
	 */
	public static class ClimateAnalysisReducer
	extends Reducer<Text,Text,Text,Text> {

		public void reduce(Text key, Iterable<Text> values,
				Context context
				) throws IOException, InterruptedException {
			double tmaxSum = 0;
			double tminSum = 0;
			int tmaxCount = 0;
			int tminCount = 0;
			String[] record;
			Text result = new Text();
			for (Text val : values) {
				record = val.toString().split(",");
				System.out.println(record[1]);
				if(record[0].equals("TMAX")) {
					tmaxSum+=Double.parseDouble(record[1]);
					tmaxCount+=Integer.parseInt(record[2]);
				}
				else {
					tminSum+=Double.parseDouble(record[1]);
					tminCount+=Integer.parseInt(record[2]);
				}
			}
			System.out.println(tmaxSum/tmaxCount);
			System.out.println(tminSum/tminCount);
			result.set(String.valueOf(tminSum/tminCount)+","+String.valueOf(tmaxSum/tmaxCount));
			context.write(key,result);
		}
	}

	/**
	 * Configuration method
	 * Combiner set to the class we have defined ClimateAnalysisCombiner
	 * Used for configuring file input and output format as well
	 * 
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "climate analysis");
		job.setJarByClass(ClimateAnalysisWithCombiner.class);
		job.setMapperClass(ClimateAnalysisMapper.class);
		job.setCombinerClass(ClimateAnalysisCombiner.class);
		job.setReducerClass(ClimateAnalysisReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
