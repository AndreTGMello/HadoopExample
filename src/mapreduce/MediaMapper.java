package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.Iterator;
 
public class MediaMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	private Text word = new Text();
	private DoubleWritable count = null;
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
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
		
		if(agregador.equals("Mes")){
			iniAgregador = 18;
			fimAgregador = 20;
		}
		
		if(agregador.equals("Semana")){
			iniAgregador = 20;
			fimAgregador = 22;
		}
		
		if(dado.equals("MedTemp")){
			iniDado = 24;
			fimDado = 30;
		}
		if(dado.equals("MedCond")){
			iniDado = 35;
			fimDado = 41;
		}
		if(dado.equals("MedMar")){
			iniDado = 46;
			fimDado = 52;
		}
		if(dado.equals("MedPressao")){
			iniDado = 57;
			fimDado = 63;
		}
		if(dado.equals("MedVento")){
			iniDado = 78;
			fimDado = 83;
		}
		if(dado.equals("MaxVento")){
			iniDado = 88;
			fimDado = 93;
		}
		if(dado.equals("MaxRajada")){
			iniDado = 95;
			fimDado = 100;
		}
		if(dado.equals("MaxTemp")){
			iniDado = 102;
			fimDado = 108;
		}
		if(dado.equals("MinTemp")){
			iniDado = 110;
			fimDado = 116;
		}
		if(dado.equals("Precip")){
			iniDado = 118;
			fimDado = 123;
		}
		if(dado.equals("Neve")){
			iniDado = 125;
			fimDado = 130;
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
			try {
				if(valorValido(valor)){
					count = new DoubleWritable(Double.parseDouble(valor));
					context.write(word, count);
				}
			} 
			catch (NumberFormatException e) {
				
			}
		}
	}

	private boolean valorValido(String valor) {
		int indice = 0;
		for(indice = 0; indice < valor.length(); indice++){
			if(valor.charAt(indice) != ' ' && valor.charAt(indice) != '9' && valor.charAt(indice)!= '.')
				return true;
		}
		return false;
	}
}