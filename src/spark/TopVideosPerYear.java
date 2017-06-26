package spark;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.spark_project.guava.collect.Lists;

import scala.Tuple2;

public class TopVideosPerYear {
	
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
    
    JavaPairRDD<String,List<Tuple2<String,Double>>> sortedRDD1 = mRDD
//    		.filter(line -> line.split("\t").length > 6)
    		.mapToPair(line -> {
    			String[] lineArr = line.split("\t");
    			String datetime = lineArr[6];
    			String year = datetime.substring(0,4);
    			String videoID = lineArr[0];
    			Double views = Double.parseDouble(lineArr[1]);
    			return new Tuple2<String,Tuple2<String, Double>>(year,new Tuple2(views,videoID));
    		}).groupByKey()
    		.mapValues(iter -> {
    			ArrayList<Tuple2<String,Double>> list = Lists.newArrayList(iter);
    			Collections.sort(list,new Comparator<Tuple2<String,Double>>() {
					@Override
					public int compare(Tuple2<String, Double> o1, Tuple2<String, Double> o2) {
						return o1._2.compareTo(o2._2);
					}
				});
    			return list.subList(0,10);
    		});
   
    long count = sortedRDD1.count();
    
    //List<Tuple2<String, Integer>> topTenTuples = sortedRDD.take(10);
    //JavaRDD<String,String,Double> topTenRdd = sc.parallelize(sortedTuple);
    
    String output_dir = "output/spark/TopVideosPerYear";
    
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