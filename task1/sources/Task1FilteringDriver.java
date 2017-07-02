package mapreduce.task1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * @author arvind
 * This class is the driver (main class) for Assignment 4.1 task 1. This is the 
 * first point of entry into the map reduce program. In this assignment, we take as
 * input a file containing television sales data and filtering out invalid records.
 * Invalid records are those records which contain 'NA' in company name/ product name  
 */
public class Task1FilteringDriver {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		// initialize the configuration
		Configuration conf = new Configuration();
		
		// create a job object from the configuration and give it any name you want 
		Job job = new Job(conf, "Assignment_4.1 -> Task_1 -> Filter_invalid_records");
		
		//java.lang.Class object of the driver class
		job.setJarByClass(Task1FilteringDriver.class);

		//map function outputs key-value pairs. What is the type of the key in the output 
		job.setMapOutputKeyClass(Text.class);
		//map function outputs key-value pairs. What is the type of the value in the output
		job.setMapOutputValueClass(Text.class);
		
		// Mapper class which has implemenation for the map phase
		job.setMapperClass(Task1FilteringMapper.class);
		
		// This is a map-only job. So number of reduceer tasks is set to zero
		job.setNumReduceTasks(0);

		// Input is a text file. So input format is TextInputFormat
		job.setInputFormatClass(TextInputFormat.class);
		
		// Output is also a text file. So output format is TextOutputFormat
		job.setOutputFormatClass(TextOutputFormat.class);

		/*
		 * The input path to the job. The map task will
		 * read the files in this path form HDFS 
		 */
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		/*
		 * The output path from the job. The reduce/map task will
		 * write the files to this path to HDFS. In this case the map task will write 
		 * to the output path because there are no reduce tasks
		 */
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		job.waitForCompletion(true);
	}
}
