package spamDetection;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;


public class Join_P_Pbar_Mapper extends Mapper<LongWritable, Text, Text, Text> {

	 @Override
	 public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException
	 {
       	FileSplit fsplit = (FileSplit) output.getInputSplit();
	    String inputFileName = fsplit.getPath().getName();
	    String s = value.toString(), from=null;
	    int i=0;
		for (String node: s.split("\t"))
		{
			if (i%2!=0)
			{
				if(inputFileName.equals("P-r-00000"))
					output.write (new Text(from), new Text("#"+node));
				else
					output.write (new Text(from), new Text("$"+node));
			}
			i++;
			from = node;
		}	
	}
}