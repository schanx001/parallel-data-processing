package secondarysort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import pdptwo.mr2.ClimateAnalysisNoCombiner;
import pdptwo.mr2.ClimateAnalysisNoCombiner.ClimateAnalysisMapper;
import pdptwo.mr2.ClimateAnalysisNoCombiner.ClimateAnalysisReducer;
import pdptwo.mr2.ClimateAnalysisNoCombiner.TemperatureWritable;
import secondarysort.SecondarySort.CompositeKeyWritable;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Secondary Sort class for calculating averages of TMAX and TMIN records for each station per year 
 * 
 * @author schanx
 *
 */
public class SecondarySort {
	
	/**
	 *  Mapper Class which emits a Composite Key consisting of stationId and year
	 *  It also ensures that only TMAX AND TMIN records are emitted
	 * @author schanx
	 *
	 */
	public static class SecondarySortMapper
	extends Mapper<Object, Text, CompositeKeyWritable, Text>{
		private String[] record;
		private Text val=new Text();
		private int year;
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				record = itr.nextToken().split(",");
				if(record[2].equals("TMAX") || record[2].equals("TMIN")) {
					val.set(record[2]+","+record[3]);
					year=Integer.parseInt(record[1].substring(0,4));
					context.write(new CompositeKeyWritable(record[0],year),val);
				}
			}
		}
	}

	/**
	 * Custom CompositeKey writable class
	 * Overriding readfields and write methods for correct serializtion and deserialization
	 * @author schanx
	 *
	 */
	public static class CompositeKeyWritable implements WritableComparable<CompositeKeyWritable>{

		public int getYear() {
			return year;
		}

		public void setYear(int year) {
			this.year = year;
		}

		public String getStationId() {
			return stationId;
		}

		public void setStationId(String stationId) {
			this.stationId = stationId;
		}


		private int year;  
		private String stationId;
		
		public CompositeKeyWritable() {

		}

		public CompositeKeyWritable(String stationId, int year){
			this.stationId= stationId;
			this.year = year;
		}

		@Override
		public String toString() {
			return (new StringBuilder().append(stationId).append(",").append(String.valueOf(year))).toString();
		}

		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeUTF(stationId);
			out.writeInt(year);
		}

		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			stationId = in.readUTF();
			year = in.readInt();
		}

		public int compareTo(CompositeKeyWritable o) {
			// TODO Auto-generated method stub
			return 0;
		}
	}
	
	/**
	 * partitioning on key stationid
	 * @author schanx
	 *
	 */
	public static class CustomPartitioner extends Partitioner<CompositeKeyWritable,Text>{

		@Override
		public int getPartition(CompositeKeyWritable key, Text value, int numPartitions) {
			// TODO Auto-generated method stub
			return Math.abs(key.getStationId().hashCode())%numPartitions;
		}
	}
	
	/**
	 * Key comparator for sorting first on stationId
	 * if stationId is equal then sort on year in increasing order
	 * 
	 * @author schanx
	 *
	 */
	public static class KeyComparator extends WritableComparator{
		protected KeyComparator() {
			super(CompositeKeyWritable.class,true);
		}
		public int compare(WritableComparable w1, WritableComparable w2) {
			CompositeKeyWritable cw1=(CompositeKeyWritable) w1;
			CompositeKeyWritable cw2=(CompositeKeyWritable) w2;
			int cmp=cw1.stationId.compareTo(cw2.stationId);
			if(cmp==0) {
				return Integer.compare(cw1.year, cw2.year);
			}
			return cmp;
		} 
	}
	
	/**
	 * Grouping comparator ensures that one reduce call is made per station
	 * Sorting on stationId only
	 * Ignores the year in the composite key
	 * @author schanx
	 *
	 */
	public static class GroupingComparator extends 	WritableComparator{
		
		public int compare(WritableComparable a, WritableComparable b) {
			// TODO Auto-generated method stub
			CompositeKeyWritable cw1=(CompositeKeyWritable) a;
			CompositeKeyWritable cw2=(CompositeKeyWritable) b;
			return cw1.stationId.compareTo(cw2.stationId);
		}

		protected GroupingComparator() {
			super(CompositeKeyWritable.class,true);
		}
		
	}
	
	/**
	 * SecondarySortReducer calculates total sum and total count for the records per year for each station
	 * Using a variable for keeping track of year change in the key so that correct sum and counts are maintained per year
	 * Resetting the variables once year changes
	 * @author schanx
	 *
	 */
	public static class SecondarySortReducer
	extends Reducer<CompositeKeyWritable,Text,Text,Text> {

		// currentYear variable to keep track of year change in key
		private int currentYear;
		protected void setup(Reducer<CompositeKeyWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			currentYear=0;
		}

		public void reduce(CompositeKeyWritable key, Iterable<Text> values,
				Context context
				) throws IOException, InterruptedException {
			StringBuilder s=new StringBuilder();
			int currentYear=0;
			double tmaxSum = 0;
			double tminSum = 0;
			int tmaxCount = 0;
			int tminCount = 0;
			String[] record;
			for (Text val : values) {
				System.out.println(key.year+" "+val);
				
				record=val.toString().split(",");
				if(currentYear==0||currentYear==key.year) {
					if(record[0].equals("TMAX")){
						tmaxSum += Double.parseDouble(record[1]);
						tmaxCount+= 1;
					}
					else {
						tminSum += Double.parseDouble(record[1]);
						tminCount+= 1;
					}
					currentYear=key.year;
				}else {
					s.append("(").append(String.valueOf(currentYear)).append(",").append(String.valueOf(tminSum/tminCount)).
					append(",").append(String.valueOf(tmaxSum/tmaxCount)).append("), ");
					//System.out.println(tminSum/tminCount);
					currentYear=key.year;
					tmaxSum=0;
					tminSum=0;
					tmaxCount=0;
					tminCount=0;
					if(record[0].equals("TMAX")){
						tmaxSum += Double.parseDouble(record[1]);
						tmaxCount+= 1;
					}
					else {
						tminSum += Double.parseDouble(record[1]);
						tminCount+= 1;
					}
					
				}
				
			}
			// appending to stringbuilder the last record for this station
			s.append("(").append(String.valueOf(currentYear)).append(",").append(String.valueOf(tminSum/tminCount)).
			append(",").append(String.valueOf(tmaxSum/tmaxCount)).append(")");
			
			// write the output to file in the form (stationId, [(year,tmin,tmax),..])
			context.write(new Text(key.stationId), new Text("["+s.toString()+"]"));
		}
	}
	
	/**
	 * Configuration method
	 * Setting custom partitioner, grouping comparator and sort comparator
	 * @param args takes input and output directories as arguments
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "climate analysis");
		job.setJarByClass(SecondarySort.class);
		job.setMapperClass(SecondarySortMapper.class);
		job.setPartitionerClass(CustomPartitioner.class);
		job.setSortComparatorClass(KeyComparator.class);
		job.setGroupingComparatorClass(GroupingComparator.class);
		job.setReducerClass(SecondarySortReducer.class);
		job.setOutputKeyClass(CompositeKeyWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	
}
