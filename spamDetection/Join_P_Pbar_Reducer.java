package spamDetection;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class Join_P_Pbar_Reducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
    	int pIndex=-1, pBarIndex=-1;
    	double p=0, pBar=0, m=0;

        for (Text value : values) 
        {
        	pIndex = value.find("#");
        	pBarIndex = value.find("$");
        	if(pIndex>=0)
        		p = Double.parseDouble (Text.decode(value.getBytes(), pIndex+1, value.toString().length()-1));
        	else if(pBarIndex>=0)
        		pBar = Double.parseDouble (Text.decode(value.getBytes(), pBarIndex+1, value.toString().length()-1));	
        	pIndex=-1;
        	pBarIndex=-1;
        }
        
        m = (p-pBar)/p;
        
        context.write(key, new Text("#"+p+",$"+pBar+",&"+m));
    }
}