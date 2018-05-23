package pdpfive.mr5;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import pdpfive.mr5.Driver.NCounter;


/**
 * @author shantanu kawlekar
 * Reducer Class for preprocessing
 */

public class PreProcessingReducer
extends Reducer<Text,Node,Text,Text> {

	
	

	Set<String> set;
	Set<String> set_pages_id;
	
	private MultipleOutputs<Text, Text> multipleOutputs;
	String outputFolder;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		set_pages_id = new HashSet<>();
		set = new HashSet<>();
		multipleOutputs = new MultipleOutputs<>(context);
		outputFolder = context.getConfiguration().get("multipleOutputs");
	}

	
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

		if(key.toString().equals("ID_GEN")) {
			
		for (Node val : values) {
				set_pages_id.add(val.getNid().toString());
			}
		}
			else {
				for (Node val : values) {
					
				// check if its not a dummy node
				if(!val.getNodeType().toString().equals("dummy")){

					context.write(key, new Text(val.getAdjList().toString()));
				}
				set.add(val.getNodeType().toString());
			}
		}

		// this adds the nodes which weren’t represented by a key, to the graph as dangling node
				if((set.contains("dummy") && set.size()==1)) {
					context.write(key, new Text(" "));
				}

		context.getCounter(NCounter.NODE_COUNTER).increment(1);
	}

	
	
	@Override
	protected void cleanup(Reducer<Text, Node, Text, Text>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		int count = 0;        
		if (outputFolder != null) {
			for (String page : set_pages_id) {
				count++;            
				multipleOutputs.write(new Text(page), new Text(""+count), outputFolder + "/page-id-map");
			}            
			multipleOutputs.close();
		} else {
			throw new IOException("Mapping output folder not found!");
		}
	}
}
