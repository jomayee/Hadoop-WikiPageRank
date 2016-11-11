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
import java.math.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.ArrayList;
import java.util.Iterator;

public class Task1 {
   
public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
      private static final Pattern pattern = Pattern.compile("\\[\\[(.+?)\\]\\]");
																			//This pattern is used to retrieve strings between [[]]
		Pattern TITLE = Pattern.compile("<title>(.*?)</title>");	//This pattern is used to retrieve the source page
		
		
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
         String value  = lineText.toString();	
		 String link="";
		 String title;
		 List<String> outLinks = new ArrayList<>();
		 //If the line contains any value that matches the title pattern, we retrieve the source using substring.
		Matcher matcher2 = TITLE.matcher(value);
		 if (matcher2.find()) {
            title = matcher2.group();
            title = title.trim();
            title = title.substring(7, title.length() - 8);
        
        //The pattern for text is searched here. If found, then pattern for [[]] is searched. For all the strings obtained in that pattern, we get the substring and add the
		//link to one array.For every word in that array, we write the output in the format <source, outlink>.
		
			Matcher matcher = pattern.matcher(value);	
			
		   while (matcher.find()) {
            String otherPage = matcher.group();
			link = otherPage.substring(2, otherPage.lastIndexOf(']') - 1);
			link=link.replace("[[", "");
				outLinks.add(link);
		   }
			for ( String word  : outLinks) {				
            if (word.isEmpty()) {
               continue;
            }			
						
			context.write(new Text(title), new Text(word));
			
						
         }
     }
		}
}
   
   public static class Reduce extends Reducer<Text ,  Text ,  Text ,  Text > {
      double rank;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context); 
        rank = context.getConfiguration().getDouble("Input", 0);
        rank = 1.0 / rank;
    }
	//The value declared in the job class is called here and rank is initialised.
      public void reduce( Text word,  Iterable<Text> values,  Context context)
         throws IOException,  InterruptedException {		
		
		boolean first = true;
		//String is added to the rank.
		String outlink=String.valueOf(rank)+"#####";
		//For every outlink present, combine the rank#####outlink1&#&#&outlink2&#&#& format.
		 for(Text value: values)
		 {
			if (!first)
				outlink += "&#&#&";
            outlink += value.toString();
			first=false;
			
		 }
		 //output is written in the format <title	rank#####outlink1&#&#&outlink2&#&#&>	
         context.write(word,  new Text(outlink));
      }
    }
	
}