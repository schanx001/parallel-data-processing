package pdpthree.mr3;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
/**
 * Intial Page Rank Mapper for initializing the page ranks
 * This is a map only job
 * @author schanx
 *
 */
public class InitialPageRankMapper
extends Mapper<Object,Text,Text,Text>{
	double initialPageRank;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		
		// TODO Auto-generated method stub
		
		long count = Long.parseLong(context.getConfiguration().get("NODECOUNT"));
		try {
			initialPageRank = (double)1/count;
		}
		catch(Exception e) {

		}
	}
	/*
	 * map function takes in records and initializes the page rank calculated in setup function for each page
	 */
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		Text val = new Text();
		Text k = new Text();
		String[] row = value.toString().split("\t");
		k.set(row[0]);
		if(!row[1].equals(" "))
			val.set(String.valueOf(initialPageRank)+","+row[1]);
		else
			val.set(String.valueOf(initialPageRank)+","+" ");
		context.write(new Text(k.toString()), val);
	}
}
