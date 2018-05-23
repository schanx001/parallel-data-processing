package pdpfive.mr5;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

/**
 * Class Driver which configures each job and sets its input output parameters
 * @author schanx
 *
 */
public class Driver {
	/*
	 * two counters for keeping track of total nodes and delta value for dangling nodes pagerank distribution
	 */
	public static enum NCounter{
		NODE_COUNTER,
		DELTA_COUNTER
	};

	/*
	 * Main function which is responsible for chaining the map reduce jobs
	 * Setting the configuration parameters needed
	 * 
	 */
	public static void main(String[] args) throws Exception {

		// Logger to write log info
		Logger log=Logger.getLogger("PageRankLogger");

		// Preprocessing job
		Path outputDir=new Path(args[1]);
		long start,end,runningTime;
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Pre Process");
		job.getConfiguration().set("multipleOutputs", args[0].replace("input", "page-id-map"));
		job.setJarByClass(Driver.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Node.class);
		job.setMapperClass(PreProcessingMapper.class);
		job.setReducerClass(PreProcessingReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));

		FileOutputFormat.setOutputPath(job,outputDir);
		job.waitForCompletion(true);

		Counters c = job.getCounters();
		long cnt = c.findCounter(NCounter.NODE_COUNTER).getValue();
		Job job1 = Job.getInstance(conf, "Generate Matrix");
		job1.getConfiguration().set("NODECOUNT", String.valueOf(cnt));
		job1.getConfiguration().set("multipleOutputs", args[0].replace("input", "r-matrix"+0));
		job1.setJarByClass(Driver.class);
		job1.setMapperClass(MatrixGenMapper.class);
		job1.addCacheFile(new URI (args[0].replace("input", "page-id-map/page-id-map-r-00000")));
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1,new Path(args[1]));

		FileOutputFormat.setOutputPath(job1,new Path(args[0].replace("input","0")));
		job1.waitForCompletion(true);
		
		// calculate PR 
		
		Path inputPR = new Path(args[1].replace("output","0"));
		Path outputPR=null;
		String taskType = "m";
		int j=0;
		String cacheFile="r-matrix0"+"/r-matrix-m-00000";

		for(int i=0;i<10;i++) {
			Job job2 = Job.getInstance(conf, "Page rank calculation");
			job2.getConfiguration().set("NODECOUNT", String.valueOf(cnt));
			j=i+1;
			outputPR = new Path(args[0].replace("input",""+j));
			job2.setJarByClass(Driver.class);
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);
			job2.setMapperClass(RowisePageRepMapper.class);
			job2.setReducerClass(RowisePageRepReducer.class);
			job2.addCacheFile(new URI(args[0].replace("input", cacheFile)));
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job2, inputPR);
			FileOutputFormat.setOutputPath(job2,outputPR);
			job2.waitForCompletion(true);
			//cacheFile = ""+j+"/part-r-00000";
			taskType="r";
		}
		
		// top 100 pages ranked based on their page ranks
		Job jobTopK = Job.getInstance(conf,"Top k Page Rank");
		jobTopK.setJarByClass(Driver.class);
		jobTopK.addCacheFile(new URI (args[0].replace("input", "page-id-map/page-id-map-r-00000")));
		
		jobTopK.setMapperClass(TopKMapper.class);
		jobTopK.setReducerClass(TopKReducer.class);
		jobTopK.setOutputKeyClass(NullWritable.class);
		jobTopK.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(jobTopK, outputPR);
		Path topk = new Path(args[0].replace("input","topkresults"));
		//if (hdfs.exists(topk))
		//	hdfs.delete(topk, true);
		FileOutputFormat.setOutputPath(jobTopK, topk);
		jobTopK.waitForCompletion(true);



	}
}
