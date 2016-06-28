package mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.Tool;

public class RegressionMapper extends Mapper<LongWritable, Text, Text, CompositeWritable> {
	private Text word = new Text();
	private CompositeWritable parXY = null;

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		word.set("FUNCTION");
		Configuration conf = context.getConfiguration();
		String estatistica = conf.get("estatistica");
		String x = "";
		double y = 0.0;

		String[] balde = value.toString().split("/t/t");

		int indexStat = 0;

		if(estatistica.equals("MEDIA")){
			indexStat = 1;
		}else if(estatistica.equals("DESVIOPADRAO")){
			indexStat = 2;
		}else if(estatistica.equals("VARIANCIA")){
			indexStat = 3;
		}

		x = balde[0];
		y = Double.parseDouble(balde[indexStat]);

		Text xWritable = new Text();
		xWritable.set(x);

		DoubleWritable yWritable = new DoubleWritable();
		yWritable.set(y);

		System.out.println("X: "+xWritable+" Y: "+yWritable);
		try {
			parXY = new CompositeWritable(xWritable, yWritable);
			context.write(word, parXY);
		} 
		catch (NumberFormatException e) {
		}

	}
}