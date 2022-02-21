package com.brainupgrade.aws.emr.hadoop.processor;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.springframework.stereotype.Component;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class WordCountProcessor extends Mapper<Object, Text, Text, IntWritable> {

  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
    StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
    Text wordOut = new Text();
    IntWritable one = new IntWritable();
    while (stringTokenizer.hasMoreElements()) {
      wordOut.set(stringTokenizer.nextToken());
      context.write(wordOut, one);
    }

  }
}
