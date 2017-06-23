/**
 * <h1>Task8_1Mapper</h1>
 * Mapper program to calculate the total units sold for each Company
 * This class will take input as (Key,Value) pair from a given file and will
 * produce the output as (Key,Value) pair.
 * */
package mapreduce.assignment6.task8;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class Task8_1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	Text outKey = new Text();
	IntWritable outValue = new IntWritable();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//every line will be split based on '|' and will be stored in String array
		String[] lineArray = value.toString().split("\\|");
		//output key for mapper class
		outKey.set(lineArray[0]);
		//output value for mapper class
		outValue.set(1);
		context.write(outKey, outValue);
	}
}