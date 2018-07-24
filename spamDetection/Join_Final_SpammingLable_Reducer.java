package spamDetection;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class Join_Final_SpammingLable_Reducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
    	int pIndex=-1, labelIndex=-1, pBarIndex=-1, mIndex=-1;
    	String pBar="", p="", label="", m="", isLabelled="";

        for (Text value : values) 
        {
        	labelIndex = value.find("%");
        	pIndex = value.find("#");
        	pBarIndex = value.find("$");
        	mIndex = value.find("&");
        	if(labelIndex>=0)
        		label = Text.decode(value.getBytes(), labelIndex+1, value.toString().length()-1);
        	else if(pIndex>=0)
        		p = Text.decode(value.getBytes(), pIndex+1, value.toString().length()-1);
        	else if(pBarIndex>=0)
        		pBar = Text.decode(value.getBytes(), pBarIndex+1, value.toString().length()-1);
        	else if(mIndex>=0)
        		m = Text.decode(value.getBytes(), mIndex+1, value.toString().length()-1);
        	pIndex=-1;
        	pBarIndex=-1;
        	mIndex=-1;
        	labelIndex=-1;
        }
        
        if (Double.parseDouble(m)>-0.45  &&  Double.parseDouble(p)>0.2)
        	isLabelled = "Good";
        else isLabelled="Bad";
        context.write(key, new Text("wasLabelled="+label+", p="+p+", pBar="+pBar+", m="+m+", isLabelled="+isLabelled));
        //context.write (key,new Text("Was Labelled: "+label+", Is Labelled: "+isLabelled));	
        //context.write(key, new Text("%wasLabelled="+label+",#p="+p+",$pBar="+pBar+",&m="+m));
    }
}