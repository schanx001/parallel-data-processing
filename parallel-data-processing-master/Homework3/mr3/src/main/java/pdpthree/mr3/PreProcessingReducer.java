package pdpthree.mr3;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import pdpthree.mr3.Driver.NCounter;

/**
 * @author shantanu kawlekar
 * Reducer Class for preprocessing
 */

public class PreProcessingReducer
extends Reducer<Text,Node,Text,Text> {

	/**
	 * Reduce function 
	 * 
	 * creates a set for values of nodes for adding the nodes which are not
	 * a part of the graph, to the graph
	 * 
	 * writes pagename and its adjacency list to output (empty string or an actual list)
	 * 
	 */
	public void reduce(Text key, Iterable<Node> values,Context context) throws IOException, InterruptedException {

		Set<String> set=new HashSet<String>();
		
		for (Node val : values) {
			// check if its not a dummy node
			if(!val.getNodeType().toString().equals("dummy")){

				context.write(key, new Text(val.getAdjList().toString()));
			}
			set.add(val.getNodeType().toString());
		}
		
		// this adds the nodes which weren’t represented by a key, to the graph as dangling node
		if((set.contains("dummy") && set.size()==1)) {
			context.write(key, new Text(" "));
		}
		
		context.getCounter(NCounter.NODE_COUNTER).increment(1);
	}

}
