package teach;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UidReducer extends Reducer<Text, Text, Text, Text> {

	private Text v = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		double sum = 0;
		for (Text v : values) {
			sum += Double.parseDouble(v.toString());
		}
		v.set(String.valueOf(sum));
		context.write(key, v);
	}

}
