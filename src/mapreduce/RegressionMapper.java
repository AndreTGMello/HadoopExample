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
		word.set("GROUP");
		Configuration conf = context.getConfiguration();
		String estatistica = conf.get("estatistica");
		String x = "";
		double y = 0.0;
		
		int iniChave = 0;
		
		String balde = value.toString();
		
		int iniEstat = 0;
		int fimEstat = 0;
		
		if(estatistica.toLowerCase().equals("media")){
			iniEstat = 5;
			fimEstat = 9;
		}
		
		if(balde.charAt(0) != 'S'){
			String valor = "";
			
			while(balde.charAt(iniChave) != '\t' && balde.charAt(iniChave) != ' '){
				valor += balde.charAt(iniChave);
				iniChave++;
			}
			x = valor;

			valor = "";
			while(iniEstat < fimEstat){
				if(balde.charAt(iniChave) != '\t' && balde.charAt(iniChave) != ' '){
					valor += balde.charAt(iniEstat);
				}
				iniEstat++;
			}
			y = Double.parseDouble(valor);
			
			try {
				parXY = new CompositeWritable(x, y);
				context.write(word, parXY);
			} 
			catch (NumberFormatException e) {
			}
		}
	}
}