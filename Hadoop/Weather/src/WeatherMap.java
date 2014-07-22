import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WeatherMap extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, IntWritable> {
	private Text key = new Text();
	private IntWritable value = new IntWritable();

	public void map(LongWritable key1, Text value1,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		try {
			String line = value1.toString();
			StringTokenizer itr = new StringTokenizer(line, " ");

			key.set(itr.nextToken().toString());
			int parseCount = 0;
			while (itr.hasMoreTokens()) {
				Text t = new Text();
				t.set(itr.nextToken());
				if (parseCount == 20 && !t.toString().contains("*")) {
					value.set(Integer.parseInt(t.toString()));
					output.collect(key, value);
				}
				parseCount++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}