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

public class RegressionMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	private Text word = new Text();
	private DoubleWritable count = null;
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		Configuration conf = context.getConfiguration();
		String estatistica = conf.get("estatistica");
		
		int iniChave = 0;
		
		String balde = value.toString();
		
		
		int iniEstat = 0;
		int fimEstat = 0;
		
		if(estatistica.toLowerCase().equals("media")){
			iniEstat = 4;
			fimEstat = 8;
		}
		
		if(balde.charAt(0) != 'S'){
			String valor = "";
			
			while(balde.charAt(iniChave) != '\t' && balde.charAt(iniChave) != ' '){
				valor += balde.charAt(iniChave);
				iniChave++;
			}
			word.set(valor);

			valor = "";
			while(iniEstat < fimEstat){
				if(balde.charAt(iniChave) != '\t' && balde.charAt(iniChave) != ' '){
					valor += balde.charAt(iniEstat);
				}
				iniEstat++;
			}
			try {
				count = new DoubleWritable(Double.parseDouble(valor));
				context.write(word, count);
			} 
			catch (NumberFormatException e) {
			}
		}
	}
}