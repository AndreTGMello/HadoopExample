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
		
		int iniChave = 0;
		
		String balde = value.toString();
		
		int iniEstat = 0;
		int fimEstat = 0;
		
		if(estatistica.equals("MEDIA")){
			iniEstat = 4;
			fimEstat = 9;
		}
		
		if(!balde.isEmpty() && balde.charAt(0) != 'S'){
			String valor = "";
			
			while(balde.charAt(iniChave) != '\t' && balde.charAt(iniChave) != ' '){
				valor += balde.charAt(iniChave);
				iniChave++;
			}
			System.out.println(valor);
			x = valor;

			valor = "";
			while(iniEstat < fimEstat){
				if(balde.charAt(iniEstat) != '\t' && balde.charAt(iniEstat) != ' '){
					valor += balde.charAt(iniEstat);
				}
				iniEstat++;
			}
			System.out.println(valor);
			y = Double.parseDouble(valor);
			
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
}