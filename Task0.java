package org.myorg;
import org.myorg.PageRank;
import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
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
import org.apache.hadoop.io.IntWritable;

public class Task0 {
   
public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
	//This pattern is used to retrieve strings between [[]]
	private final static IntWritable one = new IntWritable(1);
	//one is declared to pass it with each source, outlink, we use this count in reducer and call it in next job.
      private static final Pattern pattern = Pattern.compile("\\[\\[(.+?)\\]\\]");	
		//This pattern is used to retrieve the source page
		Pattern TITLE = Pattern.compile("<title>(.*?)</title>");
		//This pattern is used to retrieve the entire text between the text xml , this is the string which contains the outlinks information.
		
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
			
         String value  = lineText.toString();	
		 String link="";
		 String title;
		 List<String> out = new ArrayList<>();
		//If the line contains any value that matches the title pattern, we retrieve the source using substring.
		 Matcher matcher2 = TITLE.matcher(value);
		 if (matcher2.find()) {
            title = matcher2.group();
            title = title.trim();
            title = title.substring(7, title.length() - 8);
			context.write(new Text(title), one);
			//For every title, write title, one as output
        }
		//If there is no title, the job ends here.
		else{
			return;
		}
        //The pattern for [[]] is searched. For all the strings obtained in that pattern, we get the substring and add the
		//link to one array.For every word in that array, we write the output in the format <source, outlink> and simultaneously counter is incremented.
		
			Matcher matcher = pattern.matcher(value);	
			
		   while (matcher.find()) {
            String otherPage = matcher.group();
			link = otherPage.substring(2, otherPage.lastIndexOf(']') - 1);
				link=link.replace("[[", "");
				out.add(link);
		   }
			for ( String word  : out) {				
            if (word.isEmpty()) {
               continue;
            }			
			
			context.write(new Text(word), one);
				//For every outlink in a title, write outlink, one as output. This way we get the count of all nodes.	
         }
     }
		
}

public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    int count = 0;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        this.count++;
    }
	//count is initialized and incremented for every value is received. Here we use only one reducer.

    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.getCounter("Nodes", "Nodes").increment(count);
    }

}
}
   //The main purpose of this code is to just increment the counter, hence we wont be using the output of this job (<source>, <outlink>) in the next job. 
  