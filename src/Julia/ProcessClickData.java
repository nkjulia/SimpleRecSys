package Julia;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.math3.util.OpenIntToDoubleHashMap.Iterator;
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
public class ProcessClickData extends Configured implements Tool {

	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>{
		@Override
		public void map(LongWritable key,Text value,Context ctx) throws IOException,InterruptedException{
			String[] strs = value.toString().split("\t");
			String uid = strs[0];
			String appid = strs[1];
			//if(strs.length >=2 && strs[2].equals(2)){
		    ctx.write(new Text(uid),new Text(appid));
			//}
		}
	}
	public static class ReduceClass extends Reducer<Text,Text,Text,Text>{
		@Override
		public void reduce(Text key,Iterable<Text> values,Context ctx) throws IOException,InterruptedException{
			StringBuilder sb = new StringBuilder();
			Set<String> apps = new HashSet<String>();
			for(Text e:values){
				if(apps.contains(e.toString())){
					continue;
				}
				sb.append(e.toString()+"\t");
				apps.add(e.toString());
			}
			sb.delete(sb.length()-1, sb.length());
			ctx.write(key, new Text(sb.toString()));
		}
	}
	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJarByClass(ProcessClickData.class);
		
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileSystem fs = FileSystem.get(getConf());
		FileStatus[] fss = fs.listStatus(new Path(Common.INPUT_DIR));
		Arrays.sort(fss, Collections.reverseOrder());
		int len = fss.length>=Common.MAX_DAY?Common.MAX_DAY:fss.length;
		for(int i=0;i<len;i++){
			FileInputFormat.addInputPath(job,  fss[i].getPath());
			System.out.print(fss[i].getPath().toString());
		}
		
		FileOutputFormat.setOutputPath(job,new Path(Common.OUTPUT_DIR_CLICK));
		// TODO Auto-generated method stub
		return job.waitForCompletion(true)?0:1;
	}

}

