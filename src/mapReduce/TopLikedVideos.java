package mapReduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

public class TopLikedVideos extends Configured implements Tool{

	public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] lineArr = line.split("\t");
			String videoID = lineArr[0];
			Long views = Long.parseLong(lineArr[1]);
			context.write(new Text(videoID), new LongWritable(views));
		}
	}
	
	public class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> 
	{
		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			
			for(LongWritable value :values){
				context.write(key, value);
			}
		}
	}

	public int run(String[] args) throws Exception {
		BasicConfigurator.configure();
		
		System.out.println("See output in folder: " + args[1]);
		
		//remove output directory if already there
		FileSystem fs = FileSystem.get(getConf());
		fs.delete(new Path(args[1]), true); // delete file, true for recursive
		
		Job job = Job.getInstance(getConf()); 
		job.setJarByClass(TopLikedVideos.class);
		job.setJobName("TopViewedVideos");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.setInputDirRecursive(job, true); //To read recursively all sub-directories, not set by default
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		job.setMapperClass(Map.class);
//		job.setReducerClass(Reduce.class);
//		job.setCombinerClass(Combiner.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		long startTime = System.currentTimeMillis();
		boolean success = job.waitForCompletion(true);
		System.out.println("Time elapsed (sec) = " + (System.currentTimeMillis() - startTime) / 1000.0);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		TopLikedVideos driver = new TopLikedVideos();
		args = new String[] {"data", "output/mapReduce/TopViewedVideos"};
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}
	
}