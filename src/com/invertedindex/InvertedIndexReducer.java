package com.invertedindex;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(final Text key, final Iterable<Text> values, final Context context)
			throws IOException, InterruptedException {

		int threashold = 100;
		StringBuilder sb = new StringBuilder();
		String lastBook = null;
		int count = 0;

		// <keyword doc1,doc2,doc3...>

		// values <doc1, doc2, doc2, doc2, doc3,..>
		for (Text value : values) {
			if (lastBook != null && value.toString().trim().equals(lastBook)) {
				count++;
				continue;
			}

			if (lastBook != null & count < threashold) {
				count = 1;
				lastBook = value.toString().trim();
				continue;
			}
			if (lastBook == null) {
				lastBook = value.toString().trim();
				count++;
				continue;
			}

			sb.append(lastBook);
			sb.append("\t");

			count = 1;
			lastBook = value.toString().trim();
		}

		if (count >= threashold) {
			sb.append(lastBook);
		}

		if (!sb.toString().trim().equals("")) {
			context.write(key, new Text(sb.toString()));
		}
	}

}
