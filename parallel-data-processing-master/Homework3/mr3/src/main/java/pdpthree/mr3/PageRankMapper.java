package pdpthree.mr3;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import pdpthree.mr3.Driver.NCounter;
/**
 * Class PageRankMapper which emits pageranks and adjlist and accumulates delta counter for dangling nodes
 * 
 * @author schanx
 *
 */
public class PageRankMapper
extends Mapper<Object,Text,Text,Node>{
	double deltacounter;
	double nodecounter;

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {

		// TODO Auto-generated method stub

		long dcount = Long.parseLong(context.getConfiguration().get("DELTACOUNT"));

		long ncount = Long.parseLong(context.getConfiguration().get("NODECOUNT"));

		try {
			deltacounter = dcount/Math.pow(10, 8);
			nodecounter = ncount;
			//System.out.println("in setup "+deltacounter+" "+nodecounter);
		}
		catch(Exception e) {

		}
	}
	
	/*
	 * Map function 
	 * 
	 * 	Map processes the node with id n.
	 *  N stores node n’s current PageRank and its adjacency list
	 */
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		Text val = new Text();
		// random jump factor
		final Double ALPHA = 0.15;
		// link follow factor
		final Double ONE_MINUS_ALPHA = 1-ALPHA;
		
		String rowKey = value.toString().split("\t")[0];
		String[] rowVal	= value.toString().split("\t")[1].split(",");
		
		// Add contribution from dangling nodes to PageRank
		double pageRank = Double.valueOf(rowVal[0]) + (ONE_MINUS_ALPHA*deltacounter)/nodecounter;
		
		String[] adjListOutput = Arrays.copyOfRange(rowVal,1,rowVal.length);
		StringBuilder rowAdjListVal = new StringBuilder();
		for(String page:adjListOutput) {
			rowAdjListVal.append(page);
			if(adjListOutput.length>0 && !adjListOutput[0].equals(" "))
				rowAdjListVal.append(",");
		}

		if(adjListOutput.length>0 && !adjListOutput[0].equals(" "))
			rowAdjListVal.deleteCharAt(rowAdjListVal.length()-1);

		// Pass along the graph structure
		context.write(new Text(rowKey), new Node(new Text(rowKey), new Text("node"),new DoubleWritable(pageRank),new Text(rowAdjListVal.toString())));

		String[] adjList = rowAdjListVal.toString().split(",");
		double p = 0;
		
		// Compute contributions to send along outgoing links
		if(adjList.length>0 && !adjList[0].equals(" ")) {
			p=pageRank/adjList.length;
			for(String m:adjList) {
				context.write(new Text(m), new Node(new Text("pagerank"),new DoubleWritable(p)));
			}
		}
		else {
			p = pageRank;
			long pr =(long)(p*Math.pow(10, 8));
			context.getCounter(NCounter.DELTA_COUNTER).increment(pr);
		}

	}
}

