package tools;

import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class CheckDuplicate {
	
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
    
    long count1 = mRDD.filter(line -> line.split("\t").length == 7).distinct().count();
    long count2 = mRDD.filter(line -> line.split("\t").length == 7).map(line -> line.split("\t")[0]).distinct().count();
    
    String output_dir = "output";
    
    List<String> topTenTuples = mRDD.take(100);
    JavaRDD<String> topTenRdd = sc.parallelize(topTenTuples);
    
	//remove output directory if already there
	FileSystem fs = FileSystem.get(sc.hadoopConfiguration());
	fs.delete(new Path(output_dir), true); // delete dir, true for recursive
	topTenRdd.saveAsTextFile(output_dir);
    
//    System.out.println("totalCount: " + totalCount);
//	System.out.println("filteredCount: " + filteredCount);
	timeElapsed = System.currentTimeMillis() - timeElapsed;
	System.out.println("Done.Time taken (in seconds): " + timeElapsed/1000f);
	
    sc.stop();
    sc.close();
    
  }
}