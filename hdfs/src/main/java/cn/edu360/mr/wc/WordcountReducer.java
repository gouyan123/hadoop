package cn.edu360.mr.wc;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	/*key：单词；values：迭代器*/
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		int count = 0;
		Iterator<IntWritable> iterator = values.iterator();
		while(iterator.hasNext()){
			/*IntWritable就是封装了 1个 int值*/
			IntWritable value = iterator.next();
			count += value.get();
		}
		context.write(key, new IntWritable(count));
	}
}
