package pdptwo.mr2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.math3.util.MultidimensionalCounter.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This class implements In mapper combiner for mapreduce
 * @author schanx
 *
 */

public class ClimateAnalysisInMapperCombiner {
	
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
		
		private Map<String,ArrayList<Integer>> tmaxRecords = new HashMap();
		private Map<String,ArrayList<Integer>> tminRecords = new HashMap();
		private String[] record;
		private Text stationId=new Text();
		private Text recordTemperature = new Text();
		
		public Map<String, ArrayList<Integer>> getTmaxRecords() {
			return tmaxRecords;
		}

		public Map<String, ArrayList<Integer>> getTminRecords() {
			return tminRecords;
		}
		
		/*
		 * Map function which reads records one at a time and emits keys in the form of (StationId,Value)
		 * Value is a string consisting of Type of record and Temperature value
		 * Accumulator is used for the combiner to work on after mapper finishes
		 * */
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			List<Integer> accumulator;
			while (itr.hasMoreTokens()) {
				record = itr.nextToken().split(",");
				if(record[2].equals("TMAX")) {
					if(tmaxRecords.containsKey(record[0])) {
						accumulator=new ArrayList();
						accumulator.add(tmaxRecords.get(record[0]).get(0)+Integer.parseInt(record[3]));
						accumulator.add(tmaxRecords.get(record[0]).get(1)+1);
						tmaxRecords.put(record[0], (ArrayList<Integer>) accumulator);
					}else {
						accumulator = new ArrayList();
						accumulator.add(Integer.parseInt(record[3]));
						accumulator.add(1);
						tmaxRecords.put(record[0], (ArrayList<Integer>) accumulator);
					}
				}else if(record[2].equals("TMIN")) {
					if(tminRecords.containsKey(record[0])) {
						accumulator=new ArrayList();
						accumulator.add(tminRecords.get(record[0]).get(0)+Integer.parseInt(record[3]));
						accumulator.add(tminRecords.get(record[0]).get(1)+1);
						tminRecords.put(record[0], (ArrayList<Integer>) accumulator);
					}else {
						accumulator = new ArrayList();
						accumulator.add(Integer.parseInt(record[3]));
						accumulator.add(1);
						tminRecords.put(record[0], (ArrayList<Integer>) accumulator);
					}
				}
				
//					stationId.set(record[0]);
//					recordTemperature.set(record[2]+","+record[3]);
//					context.write(stationId,recordTemperature);
//				}
			}
		}
		
		// Using in mapper combiner to minimize the amount of data sent from mapper to reducer
		protected void cleanup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Map<String,ArrayList<Integer>> tmaxMap = getTmaxRecords();
			Map<String,ArrayList<Integer>> tminMap = getTminRecords();
			for(String key: tmaxMap.keySet()) {
				stationId.set(key);
				recordTemperature.set("TMAX,"+tmaxMap.get(key).get(0).toString()+","+tmaxMap.get(key).get(1));
				context.write(stationId, recordTemperature);
			}
			for(String key: tminMap.keySet()) {
				stationId.set(key);
				recordTemperature.set("TMIN,"+tminMap.get(key).get(0).toString()+","+tminMap.get(key).get(1));
				context.write(stationId, recordTemperature);
			}
		}
	}

	/**
	 * ClimateAnalysisReducer class
	 * 
	 * reduces the output directly from mapper which is combining the records locally
	 * calculates total sum and count and averages for the records per station from the partial sums and counts per mapper
	 * writes the final output to a file
	 * @author schanx
	 *
	 */
	public static class ClimateAnalysisReducer
	extends Reducer<Text,Text,Text,Text> {

		/**
		 * This reduce function calculates the average of the temperatures for both type of records
		 */
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
	 * Combiner is disabled for this program as we are using in mapper combining
	 * Used for configuring file input and output format as well
	 * 
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "climate analysis");
		job.setJarByClass(ClimateAnalysisInMapperCombiner.class);
		job.setMapperClass(ClimateAnalysisMapper.class);
		job.setReducerClass(ClimateAnalysisReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
