package vn.edu.ueh.bit.topten;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
public class TopTenMapper extends Mapper<Object, Text, Text, LongWritable> {
 private TreeMap<Long, String> record;
 @Override
 public void setup(Context context) throws IOException,
 InterruptedException {
 record = new TreeMap<Long, String>();
 }
 @Override
 public void map(Object key, Text value,
 Context context) throws IOException, InterruptedException {
 String[] tokens = value.toString().split(" ");
 String name = tokens[0];
 long count = Long.parseLong(tokens[1]);
 record.put(count, name);
 if (record.size() > 10) {
 record.remove(record.firstKey());
 }
 }
 @Override
 public void cleanup(Context context) throws IOException,
 InterruptedException {
 for (Map.Entry<Long, String> entry : record.entrySet()) {
 long count = entry.getKey();
 String name = entry.getValue();
 context.write(new Text(name), new LongWritable(count));
 }
 }
}
