package pdpthree.mr3;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * TopKReducer sorts globally all the records from locally sorted records received from map tasks
 * and keeps only top 100 in a treemap
 * @author schanx
 *
 */
public class TopKReducer extends
Reducer<NullWritable, Text, NullWritable, Text> {
	private TreeMap<Double, Text> topKPages = new TreeMap<Double, Text>();

	@Override
	public void reduce(NullWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		for (Text value : values) {
			String[] row = value.toString().split(",");
			topKPages.put(Double.parseDouble(row[1]),new Text(value));

			if (topKPages.size() > 100) {
				topKPages.remove(topKPages.firstKey());
			}
		}
		
		int i=1;
		
		// use descending order to emit top 100 pages
		for (Text k : topKPages.descendingMap().values()) {
			context.write(NullWritable.get(), new Text(i+" "+k));
			i++;
		}
	}
}
