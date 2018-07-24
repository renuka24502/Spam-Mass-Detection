package spamDetection;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class InitializePageRankMapper extends Mapper<NullWritable, BytesWritable, Text, Text> {

    @Override
    public void map(NullWritable key, BytesWritable value, Context output) throws IOException, InterruptedException
    {	
    	String s = new String(value.getBytes()), from="";
    	int numNodes = -1, i, j=0;
    	for (String line: s.split("\n"))
    	{
    		numNodes++;
    	}
    	
    	for (String line: s.split("\n"))
    	{
    		i=0;
    		if(j<numNodes)
	    		for (String node: line.split("\t"))
	    		{
	    			if (i%2!=0)
	    			{
	    				for (String to: node.split(","))
	    					output.write (new Text(from), new Text(to));
	    				output.write (new Text(from), new Text("#"+numNodes));
	    				output.write (new Text(from), new Text("$1"));
	    			}
	    			i++;
	    			from = node;
	    		}
    		j++;
    	}
    }
}