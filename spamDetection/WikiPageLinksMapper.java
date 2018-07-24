package spamDetection;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class WikiPageLinksMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
    	int i=0;
    	String s=value.toString(), from=null;
		
		for(String line:s.split("\n"))
		{
			i=0;
			for(String node: line.split(" ")) 
			{
				if (i==0)
					from = node;
				else
					context.write(new Text(from), new Text(node));
				i++;
			}
		}
    }
}