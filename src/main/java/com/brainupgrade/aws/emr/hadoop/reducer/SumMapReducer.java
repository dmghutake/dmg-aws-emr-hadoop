package com.brainupgrade.aws.emr.hadoop.reducer;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.springframework.stereotype.Component;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class SumMapReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

  public void reduce(Text term, Iterable<IntWritable> ones, Context context)
      throws IOException, InterruptedException {
    int count = 0;
    Iterator<IntWritable> iterator = ones.iterator();
    while (iterator.hasNext()) {
      count++;
      iterator.next();
    }
    IntWritable output = new IntWritable(count);
    context.write(term, output);
  }
}
