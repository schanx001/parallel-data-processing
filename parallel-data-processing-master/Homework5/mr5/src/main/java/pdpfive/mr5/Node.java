package pdpfive.mr5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
/**
 * Node writable class
 * @author schanx
 *
 */
public class Node implements Writable{

	private Text adjList;
	private Text nid;
	private DoubleWritable pageRank;
	private Text nodeType;

	// default constructor
	public Node() {		
		this.adjList = new Text();
		this.nid = new Text();
		this.pageRank = new DoubleWritable();
		this.nodeType = new Text();
	}
		
	// parameterized  constructors
	public Node(Text nid, Text adjList) {
		super();
		this.adjList = new Text(adjList);
		this.nid = new Text(nid);
		this.pageRank = new DoubleWritable();
		this.nodeType = new Text();
	}

	public Node(Text nodeType) {
		super();
		this.adjList = new Text();
		this.nid = new Text();
		this.pageRank = new DoubleWritable();
		this.nodeType = new Text(nodeType);
	}
	
	public Node(Text nid,Text nodeType, Text n) {
		super();
		this.adjList = new Text(n);
		this.nid = new Text(nid);
		this.nodeType=new Text(nodeType);
		this.pageRank=new DoubleWritable();
	}
	
	public Node(Text nodeType,DoubleWritable pageRank) {
		super();
		this.nodeType=nodeType;
		this.adjList=new Text();
		this.pageRank = pageRank;
		this.nid = new Text();
	}
	
	public Node(Text nodeType, DoubleWritable pageRank, Text n) {
		super();
		this.nodeType=new Text(nodeType);
		this.adjList = new Text(n);
		this.nid = new Text();
		this.pageRank = new DoubleWritable(pageRank.get());
	}
	
	public Node(Text nid,Text nodeType, DoubleWritable pageRank, Text n) {
		super();
		this.nodeType=new Text(nodeType);
		this.adjList = new Text(n);
		this.nid = new Text(nid);
		this.pageRank = new DoubleWritable(pageRank.get());
	}

	// getter setter methods
	public Text getNodeType() {
		return nodeType;
	}

	public void setNodeType(Text nodeType) {
		this.nodeType = nodeType;
	}

	public Text getNid() {
		return nid;
	}
	
	public Text getAdjList() {
		return adjList;
	}


	public void setNid(Text nid) {
		this.nid = nid;
	}

	public DoubleWritable getPageRank() {
		return pageRank;
	}

	public void setPageRank(DoubleWritable pageRank) {
		this.pageRank = pageRank;
	}

	// serializing and deserializing
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		nid.write(out);
		adjList.write(out);
		nodeType.write(out);
		pageRank.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		nid.readFields(in);
		adjList.readFields(in);
		nodeType.readFields(in);
		pageRank.readFields(in);
	}
	
	
	
}
