package spamDetection;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;


public class Join_Final_SpammingLable_Mapper extends Mapper<LongWritable, Text, Text, Text> {

	 @Override
	 public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException
	 {
       	FileSplit fsplit = (FileSplit) output.getInputSplit();
	    String inputFileName = fsplit.getPath().getName();
	    String s = value.toString(), from=null;
	    int i=0;
	    int pIndex=-1, pBarIndex=-1, mIndex=-1;
    	String pBar="", p="", m="";
	    
	    if(inputFileName.equals("final-r-00000"))
	    {
	    	for (String node: s.split("\t"))
			{
				if (i%2!=0)
				{
					for (String n: node.split(","))
	        		{
	        			pIndex = n.indexOf('#');
	                	pBarIndex = n.indexOf('$');
	                	mIndex = n.indexOf('&');
	                	if(pIndex>=0)
	                		p = n.substring(pIndex+1, n.length()-1);
	                	else if(pBarIndex>=0)
	                		pBar = n.substring(pBarIndex+1, n.length()-1);
	                	else if(mIndex>=0)
	                		m = n.substring(mIndex+1, n.length()-1);
	                	pIndex=-1;
	                	pBarIndex=-1;
	                	mIndex=-1;
	        		}
					output.write (new Text(from), new Text("#"+p));
					output.write (new Text(from), new Text("$"+pBar));
					output.write (new Text(from), new Text("&"+m));
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
					output.write(new Text(from), new Text("%"+node));
				}
				i++;
				from = node;
			}
		}	
	}
}