package pdpfive.mr5;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * Generates matrix for further usage in page rank calculation
 * @author schanx
 *
 */
public class MatrixGenMapper
extends Mapper<Object, Text, Text, Text>{

	Map<String,String> page_id_map;
	String outputFolder;
	MultipleOutputs<Text,Text> multipleOutputs;
	long totalNodes;
	
	// using setup to load the node counts
	// hashmap for page_id map
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		multipleOutputs = new MultipleOutputs<>(context);
		totalNodes = Long.parseLong(context.getConfiguration().get("NODECOUNT"));
		outputFolder = context.getConfiguration().get("multipleOutputs");
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
					page_id_map.put(record[0], record[1]);
				}
			}catch(Exception e) {

			}
		}
	}

	// this mapper is used to emit the matrix as individual cells 
	// and also to emit the r column vector with initial values for page ranks
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		if(!value.toString().split("\t")[1].trim().equals("")) {
			String pageName = value.toString().split("\t")[0];
			String val = value.toString().split("\t")[1];
			int outlinksNum = val.split(",").length;
			for(String page:val.split(",")) {
				if(page_id_map.get(page)!=null && page_id_map.get(pageName)!=null)
					context.write(new Text("C"), new Text(page_id_map.get(page)+";"+page_id_map.get(pageName) + ";"+String.valueOf(outlinksNum)));
			}
			multipleOutputs.write(new Text("R"), new Text(page_id_map.get(pageName)+","+String.valueOf(1.0/totalNodes)), outputFolder+"/r-matrix");
		}
	}

}
