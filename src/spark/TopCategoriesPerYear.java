package spark;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.Tuple3;

public class TopCategoriesPerYear {
	
  public static void main(String[] args) throws Exception {
    long timeElapsed = System.currentTimeMillis();
    System.out.println("Started Processing");
    SparkConf conf = new SparkConf()
    .setMaster("local")
    .setAppName("YouTubeDM");
    JavaSparkContext sc = new JavaSparkContext(conf);
    //Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
    sc.setLogLevel("ERROR");
    
    JavaRDD<String> mRDD = sc.textFile("data"); //directory where the files are
    
    JavaRDD<Tuple3<String, String, Double>> sortedRDD1 = mRDD.filter(line -> line.split("\t").length > 6)
    		.mapToPair(
    		line -> {
    			String[] lineArr = line.split("\t");
    			String datetime = lineArr[6];
    			String year = datetime.substring(0,4);
    			String category = lineArr[5];
    			Double views = Double.parseDouble(lineArr[1]);
    			Tuple2<String, String> yearcategoryTuple = new Tuple2<String, String>(year,category);
    			Tuple2<Double, Integer> viewsTuple =  new Tuple2<Double, Integer>(views,1);
    			return new Tuple2<Tuple2<String, String>,Tuple2<Double, Integer>>(yearcategoryTuple,viewsTuple);
    		})
        	.reduceByKey((x, y) -> new Tuple2<Double, Integer>(x._1 + y._1, x._2 + y._2))
        	.mapToPair(x -> new Tuple2<String,Tuple2<String,Double>>(x._1._1,new Tuple2<String,Double>(x._1._2,(x._2._1/x._2._2))))
    		.map(item -> new Tuple3<String,String,Double>(item._1,item._2._1,item._2._2));
    //        	.groupByKey();
    
   
    long count = sortedRDD1.count();
    
    //List<Tuple2<String, Integer>> topTenTuples = sortedRDD.take(10);
    //JavaRDD<String,String,Double> topTenRdd = sc.parallelize(sortedTuple);
    
    String output_dir = "output/spark/TopCategoriesPerYear";
    
	//remove output directory if already there
	FileSystem fs = FileSystem.get(sc.hadoopConfiguration());
	fs.delete(new Path(output_dir), true); // delete dir, true for recursive
	sortedRDD1.saveAsTextFile(output_dir);
	timeElapsed = System.currentTimeMillis() - timeElapsed;
	System.out.println("Done.Time taken (in seconds): " + timeElapsed/1000f);
	System.out.println("Processed Records: " + count);
    
    sc.stop();
    sc.close();
    
  }
}