package Julia;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;


import Julia.Common;
import Julia.BiArrays;

public class BigramStats extends Configured implements Tool {
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>{
		@Override
		public void map(LongWritable key,Text value,Context ctx) throws IOException,InterruptedException{
			String[] apps = value.toString().split("\t");
			StringBuilder sb = new StringBuilder();
			int i = 0;
			for(String s:apps){
				if ( i == 0 ){
					i += 1;
					continue;
				}
				 
				sb.append(s+"\t");
			}
			sb.delete(sb.length()-1, sb.length());
			for(String s:apps){
				ctx.write(new Text(s), new Text(sb.toString()));
			}
		}
	}
	public static class ReduceClass extends Reducer<Text,Text,Text,Text>{
		@Override
		public void reduce(Text key,Iterable<Text> values,Context ctx) throws IOException,InterruptedException{
			Map<String,Integer> co_occur = new HashMap<String,Integer>();
			for(Text value:values){
				String[] ss = value.toString().split("\t");
				for(String s:ss){
					Integer integer = co_occur.get(s);
					if( null == integer){
						co_occur.put(s, 1);
					}else{
						co_occur.put(s, integer + 1);
					}
				}
			}
			int k = 0;
			String[] s = new String[co_occur.size()];
			int[] cs = new int[co_occur.size()];
			for(Entry<String, Integer> e:co_occur.entrySet()){
				s[k] = e.getKey();
				cs[k] = -e.getValue(); 
				k++;
			}
			
			BiArrays.sort(cs, s);
			
			StringBuilder sb = new StringBuilder();
			for(int i=0;i<cs.length&&i<=Common.MAX_REC_CNT;i++){
				sb.append(s[i]);
				sb.append(":");
				sb.append(String.valueOf(-cs[i]));
				sb.append("\t");
			}
			sb.delete(sb.length()-1, sb.length());
			ctx.write(key, new Text(sb.toString()));
			
		}
	}
	@Override
	public int run(String[] arg0) throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJarByClass(BigramStats.class);
		job.setJobName("BigramStats##");
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		
		FileInputFormat.setInputPaths(job, new Path(Common.OUTPUT_DIR_CLICK + Common.OUTPUT_MID_CLICK));
		System.out.println(Common.OUTPUT_DIR_CLICK + Common.OUTPUT_MID_CLICK);
		FileOutputFormat.setOutputPath(job,new Path(Common.OUTPUT_DIR_BIGRAM ) );
		// TODO Auto-generated method stub
		return job.waitForCompletion(true)?0:1;
	}

}
