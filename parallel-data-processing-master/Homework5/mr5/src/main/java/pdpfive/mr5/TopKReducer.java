package pdpfive.mr5;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
	
	Map<String,String> page_id_map;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		URI[] cacheFiles = context.getCacheFiles();
		page_id_map = new HashMap<>();
		if (cacheFiles != null && cacheFiles.length > 0)
		{
			try
			{
				FileSystem fs = FileSystem.get(context.getConfiguration());
				Path path = new Path(cacheFiles[0].toString());
				BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
				String line = null;
				while((line = reader.readLine())!=null) {
					String[] record = line.split("\t");
					page_id_map.put(record[1], record[0]);
				}
			}catch(Exception e) {

			}
		}
	}

	private TreeMap<Double, Text> topKPages = new TreeMap<Double, Text>();

	@Override
	public void reduce(NullWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		System.out.println(page_id_map.size());
		//System.out.println(page_id_map.values());
		for (Text value : values) {
			String[] row = value.toString().split(",");
			System.out.println(row[0]+" "+page_id_map.get(row[0]));
			String pageName = page_id_map.get(row[0]);
			topKPages.put(Double.parseDouble(row[1]),new Text(pageName+","+row[1]));

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