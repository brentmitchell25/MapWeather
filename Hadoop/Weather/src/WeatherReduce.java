import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class WeatherReduce extends MapReduceBase implements
		Reducer<Text, WeatherWritable, Text, WeatherWritable> {

	@Override
	public void reduce(Text key, Iterator<WeatherWritable> values,
			OutputCollector<Text, WeatherWritable> output, Reporter reporter)
			throws IOException {
		double dirAverage = 0, speedAverage = 0, tempAverage = 0, dewpAverage = 0;

		int lineCount = 1;
		while (values.hasNext()) {
			WeatherWritable ww = values.next();
			dirAverage = calculateAverage(dirAverage, lineCount, ww.dir);
			speedAverage = calculateAverage(speedAverage, lineCount, ww.speed);
			tempAverage = calculateAverage(tempAverage, lineCount, ww.temp);
			dewpAverage = calculateAverage(dewpAverage, lineCount, ww.dewp);

			lineCount++;
		}

		output.collect(key, new WeatherWritable(dirAverage, speedAverage,
				tempAverage, dewpAverage));
	}

	private double calculateAverage(double average, int lineCount, double value) {
		return (average * (lineCount - 1) + value) / lineCount;
	}
}