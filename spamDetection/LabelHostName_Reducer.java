package spamDetection;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class LabelHostName_Reducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
    	int pIndex=-1, hostIndex=-1;
    	String p="", hostName="";

        for (Text value : values) 
        {
        	hostIndex = value.find("^");
        	pIndex = value.find("*");
        	if(hostIndex>=0)
        		hostName = Text.decode(value.getBytes(), hostIndex+1, value.toString().length()-1);
        	else if(pIndex>=0)
        		p = Text.decode(value.getBytes(), pIndex+1, value.toString().length()-1);
        	pIndex=-1;
        	hostIndex=-1;
        }
        
        context.write(new Text (hostName), new Text (p));
    }
}