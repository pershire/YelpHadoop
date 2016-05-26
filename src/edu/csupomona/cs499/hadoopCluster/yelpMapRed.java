package edu.csupomona.cs499.hadoopCluster;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;

import org.json.simple.*;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.*;

public class yelpMapRed extends Configured implements Tool {

	public static class Map extends
			Mapper<Text, LongWritable, Text, LongWritable> {
		public void map(Object key, Text value, Context context) {
			JSONParser parse = new JSONParser();
			try {
				JSONArray obj = (JSONArray) parse.parse(new FileReader(
						"yelp_academic_dataset.json"));
				for (Object o : obj) {
					JSONObject ob = (JSONObject) o;
					String type = (String) ob.get("type");
					if (type == "business"){
						JSONArray ar = (JSONArray) ob.get("categories");
						boolean food = false;
						for (Object c : ar){
							if (c=="Restaurants"||c=="Food"){
								food = true;
								break;
							}
						}
						if(food){
							JSONArray schools = (JSONArray) ob.get("schools");
							LongWritable rating = (LongWritable) ob.get("stars");
							for (Object s : schools){
								Text name = (Text) s;
								try {
									context.write(name,rating);
								} catch (InterruptedException e) {
									e.printStackTrace();
								}
							}
						}
					}
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				e.printStackTrace();
			}

		}
	}
	
	public static class Reduce extends Reducer<Text,LongWritable,Text,LongWritable>{
		public void reduce(Text key,Iterable<LongWritable> values,Context output) throws IOException, InterruptedException{
			int count = 0;
			long total = 0;
			for (LongWritable value : values){
				count++;
				total += value.get();
			}
			total/=count;
			output.write(key, new LongWritable(total));
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new yelpMapRed(), args);
        System.exit(res); 
	}
	
	@Override
	public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("usage: [input] [output]");
            System.exit(-1);
        }

        Job job = Job.getInstance(new Configuration());
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(yelpMapRed.class);

        job.submit();
        return 0;
    }

}
