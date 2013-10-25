package wikibooks.hadoop.chapter07;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperWithReducesideJoin extends
		Mapper<LongWritable, Text, Text, Text> {

	// 태그 선언
	public final static String DATA_TAG = "B";

	// map 출력키
	private Text outputKey = new Text();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		if (key.get() > 0) {
			// 콤마 구분자 분리
			String[] colums = value.toString().split(",");
			if (colums != null && colums.length > 0) {
				try {
					outputKey.set(colums[8] + "_" + DATA_TAG);
					context.write(outputKey, value);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
}
