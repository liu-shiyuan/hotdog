package hotdog.mr.intro;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1,
			Context context) throws IOException, InterruptedException {
		int maxValue = Integer.MAX_VALUE;
		for (IntWritable value : arg1) {
			maxValue = Math.max(maxValue, value.get());
		}
		context.write(arg0, new IntWritable(maxValue));
		FileSystem.get(new Configuration()).getFileStatus(new Path(""));
	}
}
