package pdpfive.mr5;

/**
 * @author shantanu kawlekar
 * Mapper Class for preprocessing
 */
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * 
 * @author schanx
 *
 */
public class PreProcessingMapper
extends Mapper<Object, Text, Text, Node>{



	Text nodeId = new Text();
	Text val = new Text();
	Set<String> pageSet;

	@Override
	protected void setup(Mapper<Object, Text, Text, Node>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		pageSet = new HashSet<>();

	}

	/**
	 * Map function which calls the parser function to get page and its adjacency list
	 * 
	 * Map emits (pagename , dummyObject)
	 * Map emits (pagename , node ) 
	 * 
	 */
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		// encoding page name 
		int j = value.find(":");
		String name = new String(Arrays.copyOf(value.getBytes(), j), "latin1");
		//System.out.println(name);

		Node n = null;
		n = Parser.PreProcess(value.toString(),name);

		if(n != null) {

			String[] adjList = n.getAdjList().toString().split(",");
			pageSet.add(n.getNid().toString());
			
			if(adjList.length>0 && !n.getAdjList().toString().equals(" ")) {
				// for adding nodes which are not part of the graph, to the graph 
				// pages which are not represented with keys in the given graph
				for(String page:adjList) {
					context.write(new Text(page), new Node(new Text("dummy")));
				}
			}
			// emit the page with its adjlist
			context.write(new Text(n.getNid().toString()), new Node(new Text(n.getNid().toString()),new Text("node"),new Text(n.getAdjList().toString())));

		}
	}

	@Override
	protected void cleanup(Mapper<Object, Text, Text, Node>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		for(String page:pageSet) {
			context.write(new Text("ID_GEN"), new Node(new Text(page),new Text("")));
		}
	}

}





