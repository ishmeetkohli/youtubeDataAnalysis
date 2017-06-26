package tools;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class DataCleanup {
	
  public static void main(String[] args) throws Exception {
    long timeElapsed = System.currentTimeMillis();
    System.out.println("Started Processing");
    SparkConf conf = new SparkConf()
    .setMaster("local")
    .setAppName("YouTubeDM");
    JavaSparkContext sc = new JavaSparkContext(conf);
    //Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
    sc.setLogLevel("ERROR");
    
    JavaRDD<String> mRDD = sc.textFile("data").filter(line -> line.split("\t").length == 7).distinct(); //directory where the files are
    
    JavaRDD<String> cleanedRDD = mRDD
    		.mapToPair(
    		line -> {
    			String cleanLine = line.replace("\tnull", "\t0");
    			String[] lineArr = line.split("\t");
    			String videoID = lineArr[0];
    			return new Tuple2<String, String>(videoID, cleanLine);
    		})
    		.reduceByKey((x,y)->x)
    		.map(x->x._2);
			
    long count = cleanedRDD.count();
    
    String output_dir = "output/cleanData";
    
	//remove output directory if already there
	FileSystem fs = FileSystem.get(sc.hadoopConfiguration());
	fs.delete(new Path(output_dir), true); // delete dir, true for recursive
	cleanedRDD.saveAsTextFile(output_dir);
	timeElapsed = System.currentTimeMillis() - timeElapsed;
	System.out.println("Done.Time taken (in seconds): " + timeElapsed/1000f);
	System.out.println("Processed Records: " + count);
    
    sc.stop();
    sc.close();
    
  }
}