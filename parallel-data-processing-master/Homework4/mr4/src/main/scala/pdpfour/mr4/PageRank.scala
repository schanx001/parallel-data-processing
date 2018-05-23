package pdpfour.mr4
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * @author Shantanu Kawlekar
 * Page rank in Spark using scala 
 */
object PageRank {

	/**
	 * generating graph for dataset given
	 * returns a RDD of tuples with string key-val pair
	 */
	def generateGraph(sc:SparkContext,inputPath:String):RDD[(String,String)] = {
			val graph = sc.textFile(inputPath).map(line=> Parser.PreProcess(line)).flatMap(line=> {
				var partialGraph = new ListBuffer[Tuple2[String,String]]
				if(line!=null){
				 if(line.split(":").length>1){
					val page_adjlist_pair = line.split(":")
					val pageName = page_adjlist_pair(0)
					val adjString = page_adjlist_pair(1)
					val adjList = page_adjlist_pair(1).split(",")
										
					// to get dangling nodes added to the graph
					// nodes which are in adjlsts but not as a key
					for(i<- 0 until adjList.length){
						partialGraph += new Tuple2(adjList(i),"") 
					}
								    
					var listOfOutlinks = adjString

					partialGraph+=new Tuple2(pageName,listOfOutlinks)
					}
					else{
						val pageName = line.split(":")(0)
						partialGraph+=new Tuple2(pageName,"")
					}
			 }
				
			// creating rdd of listbuffer
			partialGraph.map(x=>x)
		}).reduceByKey((a,b)=> 
			if(a!="" && b!="" && a.length()>0 && b.length()>0){
				a+","+b
			}else{
				a+b
			})
		return graph
	}


	/**
	 * calculating the pagerank for pages based on prev values from the graph
	 */
	def calculatePageRank(graphPR:RDD[(String,Double)],deltasCount:Double,nodesCount:Integer,alpha:Double):RDD[(String,Double)] = {
			val graphNewPR = graphPR.map(page=> {
			val pageRank =   (alpha/nodesCount) + (1-alpha)*(deltasCount/nodesCount)+page._2
				
				// page and new pagerank
			(page._1,pageRank)
		  })
			return graphNewPR
	}

	/**
	 * Driver program which carries out the following steps required for page rank calculation
	 * 1. generates graph pagename,adjlst
	 * 2. calculates initial page rank pagename,pagerank
	 * 3. iterates 10 times to calculated page rank values for nodes
	 * 4. calculates page rank to be distributed the outlinks of nodes
	 * 5. calculates new pageranks with given parameters and previous page rank
	 * 6. finds out top 100 pages ordering based on page ranks
	 * 7. writes output to a text file 
	 */
	def main(args: Array[String]):Unit = {
			val conf = new SparkConf()
					.setAppName("PageRank")
					.setMaster("local") // change this to yarn for aws
					val sc = new SparkContext(conf)
			
					// persist this graph(page,adjlst) data cause this is not going to change in further iterations
					val graph = generateGraph(sc,args(0)).persist(StorageLevel.MEMORY_AND_DISK)

					// counting the nodes in the graph
					val nodeCount = sc.accumulator(0,"NODE_COUNTER")

					// counting the delta nodes page rank to adjust the page rank accordingly
					val deltaCount = sc.accumulator(0.toDouble,"DELTA_COUNTER")
					val danglingNodes = sc.accumulator(0,"DANGLING_COUNTER")
					
					// calculating node count with action, so that we have node count
					graph.map(r=>{
						nodeCount+=1
								if(r._2.length()==0 || r._2==null){
									danglingNodes+=1
								}
					}).collect()

					// constant parameters
					val alpha = 0.15
					val one_minus_alpha = 1-alpha
					
					// using broadcast variable for node count
					val totalNodesAcc = sc.broadcast(nodeCount.value)
					
					// initializing page ranks
					var graphPR = graph.map(r=>{
						(r._1,1.0/totalNodesAcc.value)
					})

					val allNodes = graph.count()

					
					for(i<-0 until 10){
					  
					  // join (pagename,adjlst) with (pagename,rank)
						val pageRankD = graph.join(graphPR)
						
					  // page rank distribution
						var distributedPR = pageRankD.flatMap(page => {
							var p = 0.0
							var outlinksList = new ListBuffer[String]
							outlinksList+=page._1
							if(page._2._1!=null && page._2._1.length()>0){
									var outlinks = page._2._1.split(",")
									for(outlink <- outlinks)
									{
											outlinksList+=outlink
									}
									var prevPR = page._2._2
									p = prevPR
									p = prevPR/outlinksList.length
							}else{
											deltaCount+= page._2._2
							}
									
							// handling dangling nodes
							outlinksList.map(outlink=>{
										if(outlink!=page._1){
											(outlink,p)}
										else{
											(outlink,0.0)
							}})
						}).reduceByKey(_+_)

						val c = distributedPR.count()
								
						// calculate page rank for new iteration
						graphPR = calculatePageRank(distributedPR,deltaCount.value,totalNodesAcc.value,alpha).persist(StorageLevel.MEMORY_AND_DISK)
						
						// reset delta value for next iteration
						deltaCount.setValue(0)
					}


			// get top 100 pages by operation top(100) on rdd graphPR
			// top shuffles data internally
			// ordering based on the pagerank value
			val top100 = sc.parallelize(graphPR.top(100)(Ordering[Double].on(page=>page._2)),1)
		  top100.saveAsTextFile(args(1))
	}
}
