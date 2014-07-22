import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class WeatherReduce extends MapReduceBase implements
		Reducer<Text, IntWritable, Text, DoubleWritable> {

	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, DoubleWritable> output, Reporter reporter)
			throws IOException {

		double average = 0;
		int lineCount = 1;
		while (values.hasNext()) {
			average = (average * (lineCount - 1) + values.next().get())
					/ (double) lineCount;
			lineCount++;
		}

		output.collect(key, new DoubleWritable(average));
	}
}