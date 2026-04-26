package vn.edu.ueh.bit.average;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
public class AverageReducer extends Reducer<Text,
 LongWritable, Text, LongWritable> {
 private final LongWritable result = new LongWritable();
 @Override
 public void reduce(Text key, Iterable<LongWritable> values,
 Context context) throws IOException, InterruptedException {
 String name = key.toString();
 long sum = 0, count = 0;
 for (LongWritable val : values) {
 sum += val.get();
 count += 1;
 }
 result.set(sum / count);
 context.write(key, result);
 }
}