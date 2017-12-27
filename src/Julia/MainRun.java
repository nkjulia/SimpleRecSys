package Julia;
import Julia.ProcessClickData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import Julia.BigramStats;
public class MainRun extends Configured implements Tool{
	@Override
	public int run(String[] args) throws Exception {
		
		ProcessClickData pcd = new ProcessClickData();
		ToolRunner.run(new Configuration(),pcd, args);
		 
		FileSystem fs = FileSystem.get(getConf());
		fs.rename(new Path(Common.OUTPUT_DIR_CLICK+"/part-r-00000"), new Path(Common.OUTPUT_DIR_CLICK+Common.OUTPUT_MID_CLICK));
		fs.close();
		
		BigramStats bs = new BigramStats();
		ToolRunner.run(new Configuration(), bs,args);
		fs = FileSystem.get(getConf());
		fs.rename(new Path(Common.OUTPUT_DIR_BIGRAM+"/part-r-00000"), new Path(Common.OUTPUT_DIR_BIGRAM+Common.OUTPUT_BIGRAM));
		fs.close();
		
		return 0;
	}
	public static void main(String[] args) throws Exception{
		int res = ToolRunner.run(new Configuration(), new MainRun(), args);
		System.exit(res);
	}
}
