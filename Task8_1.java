/**
 * <h1>Task8_1</h1>
 * This class will be used to define the configuration components for the given task
 * here we are using reducer
 * the reducer will simply sum together the count value associated with each map output key
 * */
package mapreduce.assignment6.task8;

import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Task8_1 {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Task8_1");
		job.setJarByClass(Task8_1.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(Task8_1Mapper.class);
		job.setReducerClass(Task8_1Reducer.class);
		job.setCombinerClass(Task8_1Reducer.class);
		 
		job.setInputFormatClass(TextInputFormat.class);
		//using SequenceFileOutputFormat for output
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		job.waitForCompletion(true);
	}
}