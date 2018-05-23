package pdpfive.mr5;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
/**
 * Page Rank reducer
 * @author schanx
 *
 */
public class RowisePageRepReducer extends Reducer<Text,Text,Text,Text> {
	long totalNodes;
	String outputFolder;
	MultipleOutputs<Text,Text> multipleOutputs;
	
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		totalNodes = Long.parseLong(context.getConfiguration().get("NODECOUNT"));
		multipleOutputs = new MultipleOutputs<>(context);
		outputFolder = context.getConfiguration().get("multipleOutputs");
	}

	// Reducer for pagerank
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		Double rowSum = 0.0;
		Double alpha = 0.15;
		Double one_minus_alpha = 1-alpha;
		Double newPageRank=0.0;
		for(Text value:values) {
				//context.write(key, value);
				rowSum+=Double.parseDouble(value.toString());
		}
		newPageRank	 = alpha/totalNodes + one_minus_alpha*rowSum;

		context.write(new Text("R"), new Text(key.toString()+","+String.valueOf(newPageRank)));
	}
}
