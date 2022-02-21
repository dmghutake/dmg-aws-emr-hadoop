package com.dmg.aws.emr.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import com.brainupgrade.aws.emr.hadoop.processor.WordCountProcessor;
import com.brainupgrade.aws.emr.hadoop.reducer.SumMapReducer;
import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@Slf4j
public class BrainupgradeAwsEmrHadoopApplication {

  @Autowired
  WordCountProcessor wordCountProcessor;
  @Autowired
  SumMapReducer sumMapReducer;
  
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	  SpringApplication.run(BrainupgradeAwsEmrHadoopApplication.class, args);
	   Configuration configuration = new Configuration();
	   String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
	   if(otherArgs.length !=2) {
	     log.error("Usage: WrodCount<input_file> <output_directory>");
	     System.exit(2);
	   }
	   Job job =Job.getInstance(configuration,"Word Count");
	   job.setJarByClass(BrainupgradeAwsEmrHadoopApplication.class);
	   job.setMapperClass(WordCountProcessor.class);
	   job.setReducerClass(SumMapReducer.class);
	   job.setNumReduceTasks(10);
	   job.setOutputKeyClass(Text.class);
	   job.setOutputValueClass(IntWritable.class);
	   
	   FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	   FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	   boolean waitForCompletion = job.waitForCompletion(true);
	   if(waitForCompletion) {
	     System.exit(0);
	   }else {
	     System.exit(1);
	   }
		
	}

}
