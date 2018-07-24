package spamDetection;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class numGoodNodesMapper extends Mapper<NullWritable, BytesWritable, Text, Text> {

    @Override
    public void map(NullWritable key, BytesWritable value, Context output) throws IOException, InterruptedException
    {	
    	String s = new String(value.getBytes()), from="";
    	int numGoodNodes = 0, i, j=0, numRows=-1, index=0;
    	for (String line: s.split("\n")) 
    	{
    		numRows++;
    	}
    	
    	for (String line: s.split("\n")) 
    	{
    		i=0;
    		if(j<numRows)
	    		for (String node: line.split("\t"))
	    		{
	    			if (i%2!=0)
	    			{
	    				for (String to: node.split(","))
	    				{
	    					index = to.charAt(0);
	    		        	if(index=='$')
	    		        		numGoodNodes++;
	    				}
	    			}
	    			i++;
	    			from = node;
	    		}
    		j++;
    	}
    	j=0;
    	
    	for (String line: s.split("\n"))
    	{
    		i=0;
    		if(j<numRows)
	    		for (String node: line.split("\t"))
	    		{
	    			if (i%2!=0)
	    			{
	    				for (String to: node.split(","))
	    					if (to.charAt(0)!='$')
	    						output.write (new Text(from), new Text(to));
	    				output.write (new Text(from), new Text("$"+numGoodNodes));
	    				output.write (new Text(from), new Text("#"+numRows));
	    			}
	    			i++;
	    			from = node;
	    		}
    		j++;
    	}
    	//output.write(new Text("key"), new Text ("Value"));
    }
}