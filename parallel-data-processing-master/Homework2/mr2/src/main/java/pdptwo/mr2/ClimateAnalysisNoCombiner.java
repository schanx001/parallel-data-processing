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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ClimateAnalysisNoCombiner {

	/**
	 * Mapper class
	 * Checks if records are of type TMAX and TMIN
	 * Mapper emits (stationId,temperatureRecord)
	 * 
	 * @author schanx
	 *
	 */
	public static class ClimateAnalysisMapper
	extends Mapper<Object, Text, Text, TemperatureWritable>{

		//private final static IntWritable one = new IntWritable(1);
		private String[] record;
		private Text stationId=new Text();
		private TemperatureWritable temperature=new TemperatureWritable();

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				record = itr.nextToken().split(",");
				if(record[2].equals("TMAX") || record[2].equals("TMIN")) {
					temperature.setRecord(record[2].toString(), Integer.parseInt(record[3]));
					stationId.set(record[0]);
					context.write(stationId, temperature);
				}
			}
		}
	}

	/**
	 * Custom writable to write multiple values
	 * @author schanx
	 *
	 */
	public static class TemperatureWritable implements Writable{

		private int temperature;  
		private String recordType;
		private double tmin;
		private double tmax;

		public TemperatureWritable() {

		}

		TemperatureWritable(String string, int temperature){
			this.recordType = string;
			this.temperature= temperature;
		}

		TemperatureWritable(double tmin, double tmax){
			this.tmin=tmin;
			this.tmax=tmax;
		}

		public void setRecord(String recordType, int temperature) {
			this.recordType = recordType;
			this.temperature= temperature;
		}

		public void setAvgs(double tmin, double tmax) {
			this.tmin = tmin;
			this.tmax = tmax;
		}

		@Override
		public String toString() {
			return Double.toString(tmin)+","+Double.toString(tmax);
		}

		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeUTF(recordType);
			out.writeInt(temperature);
			out.writeDouble(tmin);
			out.writeDouble(tmax);
		}

		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			recordType = in.readUTF();
			temperature = in.readInt();
			tmin = in.readDouble();
			tmax = in.readDouble();
		}

		public int getTemperature() {
			return temperature;
		}

		public String getRecordType() {
			return recordType;
		}

	}

	/**
	 * ClimateAnalysisReducer class
	 * 
	 * reduces the output directly from mapper since combiner is not used
	 * calculates total sum and count and averages for the records per station
	 * writes the final output to a file
	 * @author schanx
	 *
	 */
	public static class ClimateAnalysisReducer
	extends Reducer<Text,TemperatureWritable,Text,TemperatureWritable> {

		public void reduce(Text key, Iterable<TemperatureWritable> values,
				Context context
				) throws IOException, InterruptedException {
			TemperatureWritable result = new TemperatureWritable();
			double tmaxSum = 0;
			double tminSum = 0;
			int tmaxCount = 0;
			int tminCount = 0;
			for (TemperatureWritable val : values) {
				if(val.getRecordType().equals("TMAX")){
					tmaxSum += val.getTemperature();
					tmaxCount+= 1;
				}
				else {
					tminSum += val.getTemperature();
					tminCount+= 1;
				}
			}
			System.out.println(tmaxSum/tmaxCount);

			System.out.println(tminSum/tminCount);
			result.setAvgs(tminSum/tminCount,tmaxSum/tmaxCount);
			context.write(key,result);
		}
	}
	
	/**
	 * Configuration method
	 * Combiner is disabled for this program
	 * Used for configuring file input and output format as well
	 * 
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "climate analysis");
		job.setJarByClass(ClimateAnalysisNoCombiner.class);
		job.setMapperClass(ClimateAnalysisMapper.class);
		job.setReducerClass(ClimateAnalysisReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TemperatureWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
