package mapreduce.task1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author arvind
 * This class provides the mapper implementation for Assignment 4.1 task 1
 */
public class Task1FilteringMapper extends Mapper<LongWritable, Text, Text, Text>{
	public static final String NA_STRING = "NA";
	
	
	
	/** 
	 *This method takes as input key and value. The input key and value are 
	 *determined by the InputFormat class defined in the job driver. Since in this 
	 *case it is a TextInputFormat, the input key type will be LongWritable(offset of 
	 *the line from the beginning of the file) and iput value type will be of Text(the 
	 *actual contents of the line). Various fields in a particular line are separated by 
	 *pipe(|). In every line first field is company name and second 
	 *field is product name. So splitting by pipe and checking if the first and second field 
	 *are not equal to NA. Only if they are not NA. they will be output by the mapper,
	 *thereby filtering the records in the input file   
	 */
	public void map(LongWritable lineOffsetInFile, Text recordInput, Context context) 
			throws IOException, InterruptedException {
		String[] recordFields = recordInput.toString().split("\\|");
		String companyName = recordFields[0].trim();
		String productName = recordFields[1].trim();
		if(!NA_STRING.equalsIgnoreCase(companyName) && 
				!NA_STRING.equalsIgnoreCase(productName) ) {
			context.write(recordInput, new Text());
		}
	} 
}
