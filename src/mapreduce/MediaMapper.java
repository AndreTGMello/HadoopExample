package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
 
public class MediaMapper extends Mapper<WordOffset, Text, Text, CompositeWritable> {
	private Text word = new Text();
	private CompositeWritable count = null;
	
	@Override
	protected void map(WordOffset key, Text value, Context context) throws IOException, InterruptedException{
		Configuration conf = context.getConfiguration();
		String dado = conf.get("dado");
		String agregador = conf.get("agregador");
		int iniAgregador = 0;
		int fimAgregador = 0;
		int iniDado = 0;
		int fimDado = 0;
		
		if(agregador.equals("Ano")){
			iniAgregador = 14;
			fimAgregador = 18;
		}
		
		if(dado.equals("Temperatura")){
			iniDado = 24;
			fimDado = 30;
		}
		
		
		String balde = value.toString();
		if(balde.charAt(0) != 'S'){
			String valor = "";
			
			while(iniAgregador < fimAgregador){
				valor += balde.charAt(iniAgregador);
				iniAgregador++;
			}	
			word.set(valor);
			
			valor = "";
			while(iniDado < fimDado){
				if(balde.charAt(iniDado) != ' '){
					valor += balde.charAt(iniDado);
				}
				iniDado++;
			}
			count = new CompositeWritable(Double.parseDouble(valor));
			
			try {
				context.write(word, count);
			} 
			catch (NumberFormatException e) {
				
			}
		}
	}
}