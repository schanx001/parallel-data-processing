package pdpthree.mr3;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
/**
 * Class PageRankReducer 
 * @author schanx
 *
 */
public class PageRankReducer
extends Reducer<Text,Node,Text,Text>{
	/*
	 *  Reduce receives the node object for node m and
	 *  the PageRank contributions for all m’s inlinks
	 */
	public void reduce(Text key, Iterable<Node> values,Context context) throws IOException, InterruptedException {
		double s=0;
		Node m=null;
		final Double ALPHA = 0.15;
		final Double ONE_MINUS_ALPHA = 1-ALPHA;
		
		for(Node val:values) {
			if(val.getNodeType().toString().equals("node")) {
				// The node object was found: recover graph structure
				m=new Node(val.getNodeType(), val.getPageRank(),val.getAdjList());
			}
			else {
				// A PageRank contribution from an inlink was
				// found: add it to the running sum
				s+=Double.parseDouble(val.getPageRank().toString());
			}
		}
		
		double pagerank = ALPHA/Long.parseLong(context.getConfiguration().get("NODECOUNT")) + ONE_MINUS_ALPHA*s;
		m.setPageRank(new DoubleWritable(pagerank));
		context.write(key,new Text(m.getPageRank().toString()+","+m.getAdjList().toString()));
	}
}