package mapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AggregateJob extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());
	    job.setJarByClass(getClass());
	    job.setJobName(getClass().getSimpleName());
	    
	    FileInputFormat.addInputPath(job, new Path(args[0])); //new Path(args[0]+"/*nome_da_pasta" para que arquivos dentro de pastas tambem sejam lidos
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapperClass(ProjectionMapper.class);
	    job.setCombinerClass(MediaReducer.class);
	    job.setReducerClass(MediaReducer.class);

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    
	    return job.waitForCompletion(true) ? 0 : 1;
		}
		public static void main(String[] args) throws Exception {
		    int rc = ToolRunner.run(new AggregateJob(), args);
		    System.exit(rc);
		}
}
