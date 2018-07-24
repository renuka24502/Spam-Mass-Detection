package spamDetection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

public class WikiPageRanking extends Configured implements Tool {

    private static NumberFormat nf = new DecimalFormat("00");

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new WikiPageRanking(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
    	
    	//Converts Adjacency List to Key-Value Pair
        boolean isCompleted = runXmlParsing("spamDetection/input/adjacency_list.txt", "spamDetection/ranking/convertToKeyValue");
        if (!isCompleted) return 1;
        
        //Calculate total number of nodes and initialize pageRank to (1/TotalNumNodes)
        boolean isP = initialisePageRank_P("spamDetection/ranking/convertToKeyValue", "spamDetection/ranking/p/iter00");
        if (!isP) return 1;
        
        //Join adjacency list file and spamming label file to put #nonspam on good nodes in original adjacency list only
        boolean isGoodNodes = GoodNodesDetection("spamDetection/ranking/convertToKeyValue", "spamDetection/ranking/pBar/nonSpamDetection");
        if (!isGoodNodes) return 1;
        
        //Calculate total number of good nodes and total number to nodes to initialize pageRank to (numGoodNodes/TotalNumNodes)
        boolean isPBar = initialisePageRank_PBar("spamDetection/ranking/pBar/nonSpamDetection", "spamDetection/ranking/pBar/iter00");
        if (!isPBar) return 1;

        //############ P calculation over loop for precise value ##################
        String lastResultPath = null;

        for (int runs = 0; runs < 5; runs++) {
            String inPath = "spamDetection/ranking/p/iter" + nf.format(runs);
            lastResultPath = "spamDetection/ranking/p/iter" + nf.format(runs + 1);

            isCompleted = runRankCalculation(inPath, lastResultPath);

            if (!isCompleted) return 1;
        }

        isCompleted = runRankOrdering(lastResultPath, "spamDetection/p/result", true);

        if (!isCompleted) return 1;
        // ############################################################################
        
        
        
        //############ P' calculation over loop for precise value ##################
        lastResultPath = null;

        for (int runs = 0; runs < 5; runs++) {
            String inPath = "spamDetection/ranking/pBar/iter" + nf.format(runs);
            lastResultPath = "spamDetection/ranking/pBar/iter" + nf.format(runs + 1);

            isCompleted = runRankCalculation(inPath, lastResultPath);

            if (!isCompleted) return 1;
        }

        isCompleted = runRankOrdering(lastResultPath, "spamDetection/pBar/result", false);

        if (!isCompleted) return 1;
        // ############################################################################
        
        //Join P and P' and calculate m=p-p'/p
        boolean JoinP_Pbar = Join_P_Pbar ("spamDetection/p/result", "spamDetection/final");
        if (!JoinP_Pbar) return 1;
        
        
        //False_positive & False_negative calculation
        boolean Join_final_spammingLabel = Join_Final_SpammingLable ("spamDetection/final", "spamDetection/false_p_n");
        if (!Join_final_spammingLabel) return 1;
        
        
        //HostName Labelling
        boolean LabelHostName = LabelHostName ("spamDetection/false_p_n", "spamDetection/hostnames");
        if (!LabelHostName) return 1;
        
        return 0;	
    }


    public boolean runXmlParsing(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job xmlHakker = Job.getInstance(conf, "xmlHakker");
        xmlHakker.setJarByClass(WikiPageRanking.class);
        
        xmlHakker.getConfiguration().set("mapreduce.output.basename","keyVal");

        // Input / Mapper
        FileInputFormat.addInputPath(xmlHakker, new Path(inputPath));
        xmlHakker.setMapperClass(WikiPageLinksMapper.class);
        xmlHakker.setMapOutputKeyClass(Text.class);
        xmlHakker.setMapOutputValueClass(Text.class);

        // Output / Reducer
        FileOutputFormat.setOutputPath(xmlHakker, new Path(outputPath));
        xmlHakker.setOutputFormatClass(TextOutputFormat.class);

        xmlHakker.setOutputKeyClass(Text.class);
        xmlHakker.setOutputValueClass(Text.class);
        xmlHakker.setReducerClass(WikiLinksReducer.class);

        return xmlHakker.waitForCompletion(true);
    }
    
    public boolean GoodNodesDetection(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job detectGoodNodes = Job.getInstance(conf, "GoodNodesDetection");
        detectGoodNodes.setJarByClass(WikiPageRanking.class);        

        detectGoodNodes.setOutputKeyClass(Text.class);
        detectGoodNodes.setOutputValueClass(Text.class);
        
        FileInputFormat.setInputPaths(detectGoodNodes, new Path(inputPath), new Path("/user/cloudera/spamDetection/input/spamming_label.txt"));
        FileOutputFormat.setOutputPath(detectGoodNodes, new Path(outputPath));

        detectGoodNodes.setMapperClass(GoodNodesDetectionMapper.class);
        detectGoodNodes.setReducerClass(GoodNodesDetectionReducer.class);

        return detectGoodNodes.waitForCompletion(true);
    }
    
    public boolean initialisePageRank_P(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job pageRankInitializer = Job.getInstance(conf, "pageRankInitializer");
        pageRankInitializer.setJarByClass(WikiPageRanking.class);        

        pageRankInitializer.setOutputKeyClass(Text.class);
        pageRankInitializer.setOutputValueClass(Text.class);
        
        FileInputFormat.setInputPaths(pageRankInitializer, new Path(inputPath));
        FileOutputFormat.setOutputPath(pageRankInitializer, new Path(outputPath));

        pageRankInitializer.setMapperClass(InitializePageRankMapper.class);
        pageRankInitializer.setReducerClass(InitializePageRankReducer.class);
        
        pageRankInitializer.setInputFormatClass(WholeFileInputFormat.class);

        return pageRankInitializer.waitForCompletion(true);
    }
    
    public boolean initialisePageRank_PBar(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job pageRankInitializer_pbar = Job.getInstance(conf, "pageRankInitializer_pbar");
        pageRankInitializer_pbar.setJarByClass(WikiPageRanking.class);        

        pageRankInitializer_pbar.setOutputKeyClass(Text.class);
        pageRankInitializer_pbar.setOutputValueClass(Text.class);
        
        FileInputFormat.setInputPaths(pageRankInitializer_pbar, new Path(inputPath));
        FileOutputFormat.setOutputPath(pageRankInitializer_pbar, new Path(outputPath));

        pageRankInitializer_pbar.setMapperClass(numGoodNodesMapper.class);
        pageRankInitializer_pbar.setReducerClass(InitializePageRankReducer.class);
        
        pageRankInitializer_pbar.setInputFormatClass(WholeFileInputFormat.class);

        return pageRankInitializer_pbar.waitForCompletion(true);
    }

    private boolean runRankCalculation(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job rankCalculator = Job.getInstance(conf, "rankCalculator");
        rankCalculator.setJarByClass(WikiPageRanking.class);

        rankCalculator.setOutputKeyClass(Text.class);
        rankCalculator.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(rankCalculator, new Path(inputPath));
        FileOutputFormat.setOutputPath(rankCalculator, new Path(outputPath));

        rankCalculator.setMapperClass(RankCalculateMapper.class);
        rankCalculator.setReducerClass(RankCalculateReduce.class);

        return rankCalculator.waitForCompletion(true);
    }

    private boolean runRankOrdering(String inputPath, String outputPath, boolean b) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job rankOrdering = Job.getInstance(conf, "rankOrdering");
        rankOrdering.setJarByClass(WikiPageRanking.class);
        
        if(b) rankOrdering.getConfiguration().set("mapreduce.output.basename","P");

        rankOrdering.setOutputValueClass(FloatWritable.class);
        rankOrdering.setOutputKeyClass(Text.class);

        rankOrdering.setMapperClass(RankingMapper.class);

        FileInputFormat.setInputPaths(rankOrdering, new Path(inputPath));
        FileOutputFormat.setOutputPath(rankOrdering, new Path(outputPath));

        rankOrdering.setInputFormatClass(TextInputFormat.class);
        rankOrdering.setOutputFormatClass(TextOutputFormat.class);

        return rankOrdering.waitForCompletion(true);
    }
    
    public boolean Join_P_Pbar (String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job joinP_Pbar = Job.getInstance(conf, "joinP_Pbar");
        joinP_Pbar.setJarByClass(WikiPageRanking.class);        
        
        joinP_Pbar.getConfiguration().set("mapreduce.output.basename","final");

        joinP_Pbar.setOutputKeyClass(Text.class);
        joinP_Pbar.setOutputValueClass(Text.class);
        
        FileInputFormat.setInputPaths(joinP_Pbar, new Path(inputPath), new Path("/user/cloudera/spamDetection/pBar/result"));
        FileOutputFormat.setOutputPath(joinP_Pbar, new Path(outputPath));

        joinP_Pbar.setMapperClass(Join_P_Pbar_Mapper.class);
        joinP_Pbar.setReducerClass(Join_P_Pbar_Reducer.class);

        return joinP_Pbar.waitForCompletion(true);
    }
    
    public boolean Join_Final_SpammingLable (String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
    	Configuration conf = new Configuration();

        Job joinFinal_SpammingLabel = Job.getInstance(conf, "joinFinal_SpammingLabel");
        joinFinal_SpammingLabel.setJarByClass(WikiPageRanking.class);       
        
        joinFinal_SpammingLabel.setMapperClass(Join_Final_SpammingLable_Mapper.class);
        joinFinal_SpammingLabel.setReducerClass(Join_Final_SpammingLable_Reducer.class);
        
        joinFinal_SpammingLabel.setOutputKeyClass(Text.class);
        joinFinal_SpammingLabel.setOutputValueClass(Text.class);
        
        FileInputFormat.setInputPaths(joinFinal_SpammingLabel, new Path(inputPath), new Path("/user/cloudera/spamDetection/input/spamming_label.txt"));
        FileOutputFormat.setOutputPath(joinFinal_SpammingLabel, new Path(outputPath));



        return joinFinal_SpammingLabel.waitForCompletion(true);
    }

    public boolean LabelHostName (String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
    	Configuration conf = new Configuration();

        Job label_hostName = Job.getInstance(conf, "label_hostName");
        label_hostName.setJarByClass(WikiPageRanking.class);       
        
        label_hostName.setMapperClass(LabelHostName_Mapper.class);
        label_hostName.setReducerClass(LabelHostName_Reducer.class);
        
        label_hostName.setOutputKeyClass(Text.class);
        label_hostName.setOutputValueClass(Text.class);
        
        FileInputFormat.setInputPaths(label_hostName, new Path(inputPath), new Path("/user/cloudera/spamDetection/input/hostname_label.txt"));
        FileOutputFormat.setOutputPath(label_hostName, new Path(outputPath));



        return label_hostName.waitForCompletion(true);
    }
}
