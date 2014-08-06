import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.Writable;

public class WeatherWritable implements Writable {
	public double dir;
	public double speed;
	public double temp;
	public double dewp;

	public WeatherWritable() {
	}

	public WeatherWritable(double dir, double speed, double temp, double dewp) {
		this.dir = dir;
		this.speed = speed;
		this.temp = temp;
		this.dewp = dewp;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		dir = in.readDouble();
		speed = in.readDouble();
		temp = in.readDouble();
		dewp = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(dir);
		out.writeDouble(speed);
		out.writeDouble(temp);
		out.writeDouble(dewp);
	}

	@Override
	public String toString() {
		DecimalFormat df = new DecimalFormat("#.##");
		return df.format(dir) + "  " + df.format(speed) + "  "
				+ df.format(temp) + "  " + df.format(dewp);
	}

}
