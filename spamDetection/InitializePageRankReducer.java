package spamDetection;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class InitializePageRankReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
    	String pagerank = "";
    	double n=0, x=0;
    	boolean first = true;
    	int nIndex=0, xIndex=0;

        for (Text value : values) 
        {
        	nIndex = value.find("#");
        	xIndex = value.find("$");
        	if(nIndex>=0)
        		n = Double.parseDouble (Text.decode(value.getBytes(), nIndex+1, value.toString().length()-1));
        	else if(xIndex>=0)
        		x = Double.parseDouble (Text.decode(value.getBytes(), xIndex+1, value.toString().length()-1));
        	else 
        	{
        		if(!first) pagerank += ",";
        		pagerank += value.toString();
        		first=false;
        	}
        }
        double c = x/n;

        context.write(key, new Text(c+"\t"+pagerank));
        //context.write(key, new Text(pagerank));
    }
}