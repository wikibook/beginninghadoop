package wikibooks.hadoop.chapter07;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CarrierCodeMapper extends Mapper<LongWritable, Text, Text, Text> {
	// 태그 선언
	public final static String DATA_TAG = "A";

	private Text outputKey = new Text();
	private Text outputValue = new Text();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		if (key.get() > 0) {
			String[] colums = value.toString().replaceAll("\"", "").split(",");
			if (colums != null && colums.length > 0) {
				outputKey.set(colums[0] + "_" + DATA_TAG);
				outputValue.set(colums[1]);
				context.write(outputKey, outputValue);
			}
		}
	}
}
