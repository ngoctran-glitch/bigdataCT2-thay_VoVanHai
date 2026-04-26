package vn.edu.ueh.bit.average;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
public class AverageMapper extends Mapper<Object, Text, Text, LongWritable> {
 private final LongWritable result = new LongWritable();
 @Override
 public void map(Object key, Text value,
 Context context) throws IOException, InterruptedException {
 String[] tokens = value.toString().split(" ");
 String name = tokens[0];
 long val = Long.parseLong(tokens[1]);
 result.set(val);
 context.write(new Text(name), result);
 }
}