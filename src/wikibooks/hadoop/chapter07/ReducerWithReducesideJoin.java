package wikibooks.hadoop.chapter07;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerWithReducesideJoin extends Reducer<Text, Text, Text, Text> {

	// reduce 출력키
	private Text outputKey = new Text();
	private Text outputValue = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// 태그 조회
		String tagValue = key.toString().split("_")[1];

		for (Text value : values) {
			// 출력키 설정
			if (tagValue.equals(CarrierCodeMapper.DATA_TAG)) {
				outputKey.set(value);
				// 출력값 설정 및 출력 데이터 생성
			} else if (tagValue.equals(MapperWithReducesideJoin.DATA_TAG)) {
				outputValue.set(value);
				context.write(outputKey, outputValue);
			}

		}

	}
}
