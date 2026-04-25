package vn.edu.ueh.bit.topten;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class TopTenReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
 private TreeMap<Long, String> record;
 @Override
 public void setup(Context context) throws IOException, InterruptedException {
 record = new TreeMap<Long, String>();
 }
 @Override
 public void reduce(Text key, Iterable<LongWritable> values,
 Context context) throws IOException, InterruptedException {
 String name = key.toString();
 long count = 0;
 for (LongWritable val : values) {
 count = val.get();
 }
 record.put(count, name);
 if (record.size() > 10) {
 record.remove(record.firstKey());
 }
 }

 @Override
 public void cleanup(Context context) throws IOException, InterruptedException {
 for (Map.Entry<Long, String> entry : record.entrySet()) {
 long count = entry.getKey();
 String name = entry.getValue();
context.write(new Text(name), new LongWritable(count));
 }
 }
}

