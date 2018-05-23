package pdpfive.mr5;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
/**
 * Mapper for page rank
 * @author schanx
 *
 */
public class RowisePageRepMapper extends Mapper<Object, Text, Text, Text>{
	
	Map<String,String> page_id_map;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
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
					String[] val = record[1].split(",");
					page_id_map.put(val[0], val[1]);
				}
				
			}catch(Exception e) {

			}
		}
		
	}

	// Map for page rank emits page with its cell product to be added up in reducer
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] record = value.toString().split("\t");
		String[] pageRecord = record[1].split(";");
		String pageId = pageRecord[0];
		String inlinkId = pageRecord[1];
		int outlinksLen = Integer.parseInt(pageRecord[2]);

		Double cellProduct=0.0;
		if(page_id_map.get(inlinkId)!=null) {
			cellProduct = Double.parseDouble(page_id_map.get(inlinkId))*(1.0/outlinksLen);  
		}
		context.write(new Text(pageId), new Text(String.valueOf(cellProduct)));
	}
	
}
