package org.myorg;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.myorg.Task1;
import org.myorg.Task2;
import org.myorg.Task3;
import org.myorg.Task0;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileUtil;
import java.io.File;
public class PageRank extends Configured implements Tool {

   	//Iterations are initialized
	public static int iterations=10;	
	
   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new PageRank(), args);
      System .exit(res);
   }
   
	public int run(String[] args) throws Exception {
		long Nodescount;
		//Nodescount is used to get the count value of the number of nodes
        String input="";
		String output="";
		Job job0 = new Job(getConf(), "Job0");
		job0.setJarByClass(PageRank.class);        
        // input / mapper
        FileInputFormat.addInputPath(job0, new Path(args[0]));
        job0.setMapOutputKeyClass(Text.class);
        job0.setMapOutputValueClass(IntWritable.class);
        job0.setMapperClass(Task0.Map.class);        
        FileOutputFormat.setOutputPath(job0, new Path(args[1]+"/result"));
		job0.setNumReduceTasks(1);
		//Only one reducer is used so as to get the count values.
        job0.setOutputKeyClass(Text.class);
        job0.setOutputValueClass(IntWritable.class); 
		job0.setReducerClass(Task0.Reduce.class); 
				
		boolean success = job0.waitForCompletion(true);
		Nodescount = job0.getCounters().findCounter("Nodes", "Nodes").getValue();
		//counter is called and saved
		FileSystem fs=FileSystem.get(getConf());
		fs.delete(new Path(args[1]+"/result"), true);
		//Temp directory is deleted.
		

		if (success) 
			
		{	
        
		Job job1 = new Job(getConf(), "Job1");
		job1.setJarByClass(PageRank.class);        
        // input / mapper
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setMapperClass(Task1.Map.class);  
			
        // output / reducer
		job1.getConfiguration().set("Input", "" + Nodescount);
		//Previous job value is called and set here, we will call this value in reducer for this job.
        FileOutputFormat.setOutputPath(job1, new Path(args[1]+"/result0"));
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setReducerClass(Task1.Reduce.class);		
        success = job1.waitForCompletion(true);
		
		
		if (success) 
		
		{
		//This job is run for 10 iterations and for every iteration, based on forloop the input and output directories change. 
		
        for (int iter = 0; iter < iterations; iter++) {
            input = args[1] + "/result" + iter;
            output = args[1] + "/result" + (iter+1);
            Job job2 = new Job(getConf(), "Job2");
			job2.setJarByClass(PageRank.class);
			
        FileInputFormat.setInputPaths(job2, input);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setMapperClass(Task2.Map2.class);
        
        FileOutputFormat.setOutputPath(job2, new Path(output));
        job2.setOutputFormatClass(TextOutputFormat.class);
        job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setReducerClass(Task2.Reduce2.class);
		success = job2.waitForCompletion(true);
		//Once the job in each iteration is successfully finished, the input directory is deleted.
		FileSystem fs1=FileSystem.get(getConf());
		fs1.delete(new Path(args[1]+"/result"+iter), true);
		}
		if (success) 
	
		{
		Job job3 = new Job(getConf(), "Job3");
		job3.setJarByClass(PageRank.class);        
        // input / mapper
        FileInputFormat.addInputPath(job3, new Path(args[1]+"/result10"));
        job3.setMapOutputKeyClass(DoubleWritable.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setMapperClass(Task3.Map3.class);        
        // output / reducer
        FileOutputFormat.setOutputPath(job3, new Path(args[1]+"/finalresult"));
        job3.setOutputKeyClass(DoubleWritable.class);
        job3.setOutputValueClass(Text.class);
        job3.setReducerClass(Task3.Reduce3.class);
        job3.setNumReduceTasks(1);
		//Only one reducer is used so as to get the finalranks in sorted order.
		success = job3.waitForCompletion(true);
		FileSystem fs2=FileSystem.get(getConf());
		//the input directory is deleted.
		fs2.delete(new Path(args[1]+"/result10"), true);
		
		}
		}
		}
return 0;		
		
		}
}


		
		
		