package spamDetection;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;


public class LabelHostName_Mapper extends Mapper<LongWritable, Text, Text, Text> {

	 @Override
	 public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException
	 {
       	FileSplit fsplit = (FileSplit) output.getInputSplit();
	    String inputFileName = fsplit.getPath().getName();
	    String s = value.toString(), from=null;
	    int i=0;
	    
	    if(inputFileName.equals("hostname_label.txt"))
	    {
	    	for (String node: s.split(" "))
			{
				if (i==1)
				{
					output.write(new Text (from), new Text ("^"+node));
				}
				i++;
				from = node;
			}
	    }
	    else
		{
			for (String node: s.split("\t"))
			{
				if (i==1)
				{
					output.write(new Text(from), new Text("*"+node));
				}
				i++;
				from = node;
			}
		}	
	}
}