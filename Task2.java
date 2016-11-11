package org.myorg;
import org.myorg.PageRank;
import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import org.apache.commons.lang.StringUtils;
import java.nio.charset.CharacterCodingException;
import java.lang.*;
import java.util.*;

public class Task2 {
	
public static class Map2 extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
      	  
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
								// Input format is <key(offset)		(title	rank#####outlink1&#&#&outlink2&#&#&)>
		String sep=":";
         String value  = lineText.toString();		//converted the text to string
		 String[] list=value.split("\t");		//split by tab
		String source=list[0];					//retrieve the title
			String[] list1=list[1].split("#####");			//split the rank#####outlink1&#&#&outlink2&#&#&
			double rank=Double.parseDouble(list1[0]);			//convert the rank to double
			String links="";					
			if  (list1.length>1)								//after splitting by #####, if the list length is more than one, it is clear, there is an outlink present or else no. 
			{
				links=list1[1];									
				String[] list2=list1[1].split("&#&#&");			//outlink1&#&#&outlink2&#&#& is split by seperator 
				double ranks = rank / list2.length;				//Rank of each node is calculated as PR/No. of outlinks
				
			for ( String word  : list2) {				
															//For every outlinknode, write the output with rank.
				context.write(new Text(word), new Text("" + ranks));
			
         }
			}
																				//For every source, write source, :outlinks list as output.
		 context.write(new Text(source), new Text(sep+links));
     }
		
		}

				
	public static class Reduce2 extends Reducer<Text ,  Text ,  Text ,  Text > {
      
	   public void reduce( Text word,  Iterable<Text> values,  Context context)
		 throws IOException,  InterruptedException {
		double rank2 = 0.0;										//initial rank is given
		String links="";
		String sep=":";
		for (Text value : values) { 
            String line = value.toString();
			if (line.startsWith(sep))						//if line starts with : seperator, declared in mapper, strip the seperator and add all the links. 
			{
				links+=line.substring(1);
			}
			else if (!line.equals("") && !line.startsWith(sep))			//else add the ranks of these.
				
			{
				
				rank2+=Double.parseDouble(line.toString());
				
			}
		}
			
		double newRank = 0.85* rank2 + 0.15;				//use the rank calculated above to find the final rank and pass it as newrank for next iteration.
        context.write(word, new Text(newRank + "#####" + links));
   }
	}
}