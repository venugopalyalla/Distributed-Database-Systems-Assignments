import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class equijoin {
	public static String table1 = "";
	public static String table2 = "";

	public static class MapperClass extends MapReduceBase implements
			Mapper<LongWritable, Text, DoubleWritable, Text> {
		public void map(LongWritable key, Text line,
				OutputCollector<DoubleWritable, Text> output, Reporter reporter)
				throws IOException {
			String str = line.toString();
			String splits[] = str.split(",");
			Text value = new Text(str);

			if (table1.equals(""))
				table1 = splits[0];
			else if (!table1.equals(splits[0]))
				table2 = splits[0];
			DoubleWritable joinkey = new DoubleWritable(
					Double.parseDouble(splits[1]));
			output.collect(joinkey, value);
		}
	}

	public static class ReducerClass extends MapReduceBase implements
			Reducer<DoubleWritable, Text, Text, Text> {
		public void reduce(DoubleWritable key, Iterator<Text> lines,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			HashSet<String> hSet1 = new HashSet<String>();
			HashSet<String> hSet2 = new HashSet<String>();

			while (lines.hasNext()) {
				String str = lines.next().toString();
				String[] strsplits = str.split(",");
				String table = strsplits[0];
				if (table.equals(table1))
					hSet1.add(str);
				else if (table.equals(table2))
					hSet2.add(str);
			}

			for (int i = 0; i < hSet1.size(); i++) {
				for (int j = 0; j < hSet2.size(); j++) {
					String str1 = String.join(", ", hSet1);
					String str2 = String.join(", ", hSet2);
					String linestr = str1 + ", " + str2;
					Text opline = new Text(linestr);
					output.collect(new Text(""), opline);
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(equijoin.class);
		conf.setJobName("equijoin");

		conf.setMapperClass(MapperClass.class);
		conf.setReducerClass(ReducerClass.class);

		conf.setOutputKeyClass(DoubleWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.set("mapred.textoutputformat.separator", " ");

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}