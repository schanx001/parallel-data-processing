package pdpthree.mr3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
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
		start = System.currentTimeMillis();
		Job job = Job.getInstance(conf, "Pre Process");
		job.setJarByClass(Driver.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Node.class);
		job.setMapperClass(PreProcessingMapper.class);
		job.setReducerClass(PreProcessingReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//FileSystem hdfs = FileSystem.get(conf);
		//if (hdfs.exists(outputDir))
		//	hdfs.delete(outputDir, true);

		FileOutputFormat.setOutputPath(job,outputDir);
		job.waitForCompletion(true);
		
		end=System.currentTimeMillis();
		runningTime = end-start; 
		log.info("Pre processing running time is:"+runningTime);
		
		Counters c = job.getCounters();
		long cnt = c.findCounter(NCounter.NODE_COUNTER).getValue();
		Path outputDir2=new Path("pagerank0");
		
		
		start = System.currentTimeMillis();
		
		Job jobIntialPR = Job.getInstance(conf,"Initial Page Rank");
		jobIntialPR.getConfiguration().set("NODECOUNT", String.valueOf(cnt));
		jobIntialPR.setJarByClass(Driver.class);
		jobIntialPR.setMapperClass(InitialPageRankMapper.class);
		jobIntialPR.setOutputKeyClass(Text.class);
		jobIntialPR.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(jobIntialPR, new Path(args[1]));

		//if (hdfs.exists(outputDir2))
		//	hdfs.delete(outputDir2, true);
		FileOutputFormat.setOutputPath(jobIntialPR, new Path(args[0].replace("input","pagerank0")));
		jobIntialPR.waitForCompletion(true);
		
		end=System.currentTimeMillis();
		runningTime = end-start; 
		log.info("Initializing Pageranks running time is:"+runningTime);
		
		
		conf.set("NODECOUNT", String.valueOf(cnt));
		int outputNo=0;
		//long deltacnt = c.findCounter(NCounter.DELTA_COUNTER).getValue();
		long oldDelta=c.findCounter(NCounter.DELTA_COUNTER).getValue();

		Path inputPR = new Path(args[1].replace("output","pagerank0"));
		int i;
		Path outputPR=null;
		
		start = System.currentTimeMillis();
		
		// page rank calculation job in 10 iterations
		for(i=0;i<10;i++) {
			conf.set("DELTACOUNT", String.valueOf(oldDelta));
			Job jobPR = Job.getInstance(conf,"Page Rank ");
			outputNo=i+1;
			outputPR = new Path(args[0].replace("input","pagerank"+outputNo));
			jobPR.setJarByClass(Driver.class);
			jobPR.setMapOutputKeyClass(Text.class);
			jobPR.setMapOutputValueClass(Node.class);
			jobPR.setMapperClass(PageRankMapper.class);
			jobPR.setReducerClass(PageRankReducer.class);
			jobPR.setOutputKeyClass(Text.class);
			jobPR.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(jobPR, inputPR);
			//if (hdfs.exists(outputPR))
			//	hdfs.delete(outputPR, true);
			FileOutputFormat.setOutputPath(jobPR, outputPR);
			jobPR.waitForCompletion(true);
			oldDelta =  jobPR.getCounters().findCounter(NCounter.DELTA_COUNTER).getValue();
			jobPR.getCounters().findCounter(NCounter.DELTA_COUNTER).setValue(0);
			inputPR=outputPR;
		}
		
		end=System.currentTimeMillis();
		runningTime = end-start; 
		log.info("Calculating Page rank running time is:"+runningTime);
		
		
		start = System.currentTimeMillis();
		
		// top 100 pages ranked based on their page ranks
		Job jobTopK = Job.getInstance(conf,"Top k Page Rank");
		jobTopK.setJarByClass(Driver.class);
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

		end=System.currentTimeMillis();
		runningTime = end-start; 
		log.info("Finding top k running time is:"+runningTime);
		
	}
}
