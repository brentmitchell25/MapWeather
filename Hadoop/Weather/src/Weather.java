import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class Weather {

	public static void main(String[] args) {
		try {
			JobClient client = new JobClient();
			JobConf conf = new JobConf(Weather.class);

			conf.setJobName("Weather");

			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(IntWritable.class);

			FileInputFormat.addInputPath(conf, new Path("noaa.txt"));
			FileOutputFormat.setOutputPath(conf, new Path("output3"));

			conf.setMapperClass(WeatherMap.class);
			conf.setReducerClass(WeatherReduce.class);

			client.setConf(conf);

			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}