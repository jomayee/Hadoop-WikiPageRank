//Jyothirmayi Panda (800932963), jpanda@uncc.edu

package org.myorg;
import org.myorg.PageRank;
import java.lang.*;
import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import java.math.*;
import java.util.ArrayList;
import org.apache.hadoop.mapred.JobConf;
import java.util.List;
import java.util.Set;
import java.util.Arrays;
import static java.lang.Math.abs;
import static java.lang.Math.*;

public class Task3 {
   
public static class Map3 extends Mapper<LongWritable ,  Text ,  DoubleWritable ,  Text > {
            	  
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
		
         String line  = lineText.toString();
		 if (line.isEmpty()) {
               return;
            }	
		String[] parts=line.split("\t");		//Input is split with tab
		String page=parts[0];				//source is taken
		String[] data=parts[1].split("#####");		//the rank and outlink list is split.
		
		double result=Double.parseDouble(data[0].toString());		//rank is converted to double and multiple by -1. we are doing this so that mapper writes all lines in descending order.
		
		result*=-1;
		 		          
			context.write(new DoubleWritable(result), new Text(page));
         }
      }

   public static class Reduce3 extends Reducer<DoubleWritable ,  Text ,  Text ,  DoubleWritable > {
		
      public void reduce( DoubleWritable value,  Iterable<Text> counts,  Context context)
         throws IOException,  InterruptedException {
         		 
         for ( Text count  : counts) {						//only one reducer is used and this gets all the lines in descending order. it just takes the
															//absolute value of the rank and gives as output.
			double result1=Double.parseDouble(value.toString());
			 double value2=Math.abs(result1);
			
			 context.write(count, new DoubleWritable(value2));
			 }
      }
   }

}

