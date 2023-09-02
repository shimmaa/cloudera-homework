import scala.math.pow
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf

case class Lab16Results(convergence:Double,kPoints:Array[(Double,Double)])
trait Lab16Stub extends Serializable{
  //======================== UTILITY METHODS=========================    
// The squared distances between two points
def distanceSquared(p1: (Double,Double), p2: (Double,Double)) = {
  pow(p1._1 - p2._1,2) + pow(p1._2 - p2._2,2 )
}
 
// The sum of two points
def addPoints(p1: (Double,Double), p2: (Double,Double)) = {
  (p1._1 + p2._1, p1._2 + p2._2)
}
 
// for a point p and an array of points, return the index in the array of the point closest to p
def closestPoint(p: (Double,Double), points: Array[(Double,Double)]): Int = {
    var index = 0
    var bestIndex = 0
    var closest = Double.PositiveInfinity

    for (i <- 0 until points.length) {
      val dist = distanceSquared(p,points(i))
      if (dist < closest) {
        closest = dist
        bestIndex = i
      }
    }
    bestIndex
}
  //===================================================================
  def run(rawData:RDD[String]):Lab16Results
}
object Lab16 extends Lab16Stub{
    
 def debugRDD[T](title: String, rdd:RDD[T], n:Int=5){
   println(s"======= $title =======")
   rdd.take(n)foreach(println)
   println("=====================")
  }
  /**
   * Refer to lap 16 homework sheet and implement this method
   * you can use debugRDD to print the first n rows (n = 5 by default)
   * @return a case class containing :
   * <ol>
   * <li> The convergence value that stops the iteration </li>
   * <li> The final k means, each mean is represented by a tuple of (lat,long)</li> 
   * </ol> 
   */
  def run(rawData:RDD[String]):Lab16Results={     
    // ========YOUR CODE GOES HERE===========
    //************************ PREPARE DATA ********************
    //YOU NEED TO HAVE /loudacre/devicestatus_etl folder ready in your hdfs
    //PLEASE REFER TO THE SCRIPT IN http://pastebin.com/CmKd0ayP FOR HELP
    //**********************************************************
    println("Lab16 : K-Means Clustering")
    debugRDD("Debuging Input Data",rawData)
    // K is the number of means (center points of clusters) to find
    val K = 5
 
    // ConvergeDist -- the threshold "distance" between iterations at which we decide we are done
    val convergeDist = .1
    
    // Parse the device status data file into pairs
    // TODO
      var data = rawData.map(line => line.split(',')).map(fields => (fields(3).toDouble, fields(4).toDouble)).filter{case(v1,v2)=> v1!= 0 && v2!=0}.cache()
    
    //start with K randomly selected points from the dataset
    //TODO
    var kPoints:Array[(Double,Double)] = data.takeSample(false, K, 42)       // set the value with K random points

    //  loop until the total distance between one iteration's points and the next is less than the convergence distance specified
    var tempDist = Double.PositiveInfinity
    
    while (tempDist > convergeDist) {
      
      // for each point, find the index of the closest kpoint.  map to (index, (point,1))
      //TODO
	var data_a = data.map(line => (closestPoint(line,kPoints),(line,1)))
       
      // For each key (k-point index), reduce by adding the coordinates and number of points
      //TODO
	var data_b = data_a.reduceByKey((x,y) => (addPoints(x._1,y._1),x._2+y._2)).sortByKey()
   
      // For each key (k-point index), find a new point by calculating the average of each closest point
      //TODO
	var data_c = data_b.map{case(index,((totalX,totalY),n)) => (index,(totalX/n,totalY/n))}
   
      // calculate the total of the distance between the current points and new points
      // TODO

        var nkPoints = data_c.map(pair => pair._2).collect()
	tempDist = (nkPoints zip kPoints).map(pair => distanceSquared(pair._1,pair._2)).sum

      // Copy the new points to the kPoints array for the next iteration
      // TODO
	//kPoints = nkPoints
	Array.copy(nkPoints,0,kPoints,0,kPoints.length)
      
      //TODO : REMOVE THIS LINE TO CONTINUE LOOPING
      //tempDist= -1.0;
    }
     println(s"======= The final K center points =======")
     kPoints.foreach(println)
     println("=====================")
    println("Lab16 DONE!")
    return Lab16Results(tempDist,kPoints)
  }
  def runOnShell(sc:SparkContext){
    // Disabling verbose messages
    org.apache.log4j.Logger.getRootLogger.setLevel(org.apache.log4j.Level.WARN)
    val file = "hdfs://localhost:8020/loudacre/devicestatus_etl";
    val data = sc.textFile(file); 
    run(data)
  }
  // YOU CAN RUN THIS FILE FROM ECLIPSE BY : right click on the file and : Run As-> Scala Application
   def main(args: Array[String]): Unit = { 
     val conf = new org.apache.spark.SparkConf()
      .setAppName("The swankiest Spark app ever")
      .setMaster("local[1]")
     val sc  =  new SparkContext(conf)
     runOnShell(sc);
  }
}
// To test your code on spark-shell, run those lines in the spark-shell 
// :load Lab16.scala
// Lab16.runOnShell(sc)
 

