import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MapWeather {

	public static void main(String[] args) {
		try {
			JobClient client = new JobClient();
			JobConf conf = new JobConf(MapWeather.class);

			conf.setJobName("MapWeather");

			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(conf, new Path("noaa.txt"));
			FileOutputFormat.setOutputPath(conf, new Path("output"));

			conf.setMapperClass(MapClass.class);

			client.setConf(conf);

			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static class MapClass extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		private Text key = new Text();
		private Text value = new Text();

		public void map(LongWritable key1, Text value1,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			try {
				String line = value1.toString();
				StringTokenizer itr = new StringTokenizer(line);

				key.set(itr.nextToken().toString());

				while (itr.hasMoreTokens()) {
					value.set(itr.nextToken());
					output.collect(key, value);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}