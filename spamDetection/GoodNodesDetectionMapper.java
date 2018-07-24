package spamDetection;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;


public class GoodNodesDetectionMapper extends Mapper<LongWritable, Text, Text, Text> {

	 @Override
	 public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException
	 {
       	FileSplit fsplit = (FileSplit) output.getInputSplit();
	    String inputFileName = fsplit.getPath().getName();
	    String s = value.toString(), from=null;
	    int i=0;
		if(inputFileName.equals("keyVal-r-00000"))
		{
			for (String node: s.split("\t"))
    		{
    			if (i%2!=0)
    			{
    				for (String to: node.split(","))
    					output.write (new Text(from), new Text(to));
    			}
    			i++;
    			from = node;
    		}		
		}
		else
		{
			for (String node: s.split(" "))
			{
				if (i==1)
				{
					if (node.equals("nonspam"))
						output.write(new Text(from), new Text("$"+node));
				}
				i++;
				from = node;
			}
		}
	}
}