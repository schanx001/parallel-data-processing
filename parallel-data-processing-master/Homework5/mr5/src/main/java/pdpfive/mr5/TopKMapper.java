package pdpfive.mr5;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * TopKMapper sorts locally all the records and keeps only top 100 in a treemap
 * @author schanx
 *
 */
public class TopKMapper extends
Mapper<Object, Text,NullWritable, Text> {
	// Our output key and value Writables
	private TreeMap<Double, Text> topKPages = new TreeMap<Double, Text>();

	
	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// Parse the input string into a nice map
		String[] row;
		String rowKey;
		String rowVal;
		row = value.toString().split("\t"); 
		rowKey = row[1].split(",")[0];
		rowVal = row[1].split(",")[1];

		// formatting to 15th decimal place
		String pagerank = String.format("%.15f", Double.parseDouble(rowVal));
		
		topKPages.put(Double.parseDouble(pagerank), new Text(rowKey +"," + pagerank));

		// removing lowest key
		if (topKPages.size() > 100) {
			topKPages.remove(topKPages.firstKey());
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
	InterruptedException {

		for (Text t : topKPages.values()) {
			context.write(NullWritable.get(), t);
		}
	}

}

