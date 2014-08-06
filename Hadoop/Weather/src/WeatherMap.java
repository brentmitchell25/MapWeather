import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WeatherMap extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, WeatherWritable> {
	private final Text key = new Text();
	private WeatherWritable value;

	@Override
	public void map(LongWritable inputKey, Text inputValue,
			OutputCollector<Text, WeatherWritable> output, Reporter reporter)
			throws IOException {
		try {
			String line = inputValue.toString();
			StringTokenizer itr = new StringTokenizer(line, " ");

			key.set(itr.nextToken().toString());
			int parseCount = 0;
			double dir = 0, speed = 0, temp = 0, dewp = 0;

			while (itr.hasMoreTokens()) {
				Text t = new Text();
				t.set(itr.nextToken());
				if (!t.toString().contains("*")) {
					if (parseCount == 2)
						dir = Double.parseDouble(t.toString());
					else if (parseCount == 3)
						speed = Double.parseDouble(t.toString());
					else if (parseCount == 20)
						temp = Double.parseDouble(t.toString());
					else if (parseCount == 21) {
						dewp = Double.parseDouble(t.toString());

						break;
					}
				}
				parseCount++;
			}
			value = new WeatherWritable(dir, speed, temp, dewp);
			output.collect(key, value);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}