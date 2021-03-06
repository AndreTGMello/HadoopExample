package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;
 
public class StatisticsMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	private Text word = new Text();
	private DoubleWritable count = null;
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		Configuration conf = context.getConfiguration();
		String dado = conf.get("dado");
		String agregador = conf.get("agregador");
		int iniDado = 0;
		int fimDado = 0;
		//determinacao de onde encontra-se cada atributo dos arquivos de entrada
		if(dado.equals("MEDTEMP")){
			iniDado = 24;
			fimDado = 30;
		}
		if(dado.equals("MEDCOND")){
			iniDado = 35;
			fimDado = 41;
		}
		if(dado.equals("MEDMAR")){
			iniDado = 46;
			fimDado = 52;
		}
		if(dado.equals("MEDPRESSAO")){
			iniDado = 57;
			fimDado = 63;
		}
		if(dado.equals("MEDVENTO")){
			iniDado = 78;
			fimDado = 83;
		}
		if(dado.equals("MAXVENTO")){
			iniDado = 88;
			fimDado = 93;
		}
		if(dado.equals("MAXRAJADA")){
			iniDado = 95;
			fimDado = 100;
		}
		if(dado.equals("MAXTEMP")){
			iniDado = 102;
			fimDado = 108;
		}
		if(dado.equals("MINTEMP")){
			iniDado = 110;
			fimDado = 116;
		}
		if(dado.equals("PRECIP")){
			iniDado = 118;
			fimDado = 123;
		}
		if(dado.equals("NEVE")){
			iniDado = 125;
			fimDado = 130;
		}
		
		String balde = value.toString();
		if(balde.charAt(0) != 'S'){ //pula a primeira linha de cada arquivo
			String valor = "";
			valor += preencheData(agregador, balde);
			word.set(valor);//insere a data na tupla como definido pelo usuario
			
			valor = "";
			while(iniDado < fimDado){
				if(balde.charAt(iniDado) != ' '){//pula entradas vazias ou espacos em campos com valores
					valor += balde.charAt(iniDado);
				}
				iniDado++;
			}
			try {
				if(valorValido(valor)){// se o valor for valido, insere-se na tupla
					count = new DoubleWritable(Double.parseDouble(valor));
					context.write(word, count);
				}
			} 
			catch (NumberFormatException e) {
				
			}
		}
	}

	private String preencheData(String agregador, String balde) {//para a key da tupla, gera-se uma data
		DecimalFormat doisDigitos = new DecimalFormat("00");     //conforme definido pelo usuario
		int iniAgregador = 0;//para acessar o atributo YEARMODA do arquivo de entrada
		int fimAgregador = 0;//no ponto correto de acordo com a definicao do usuario
		String valor = "";
		int numeroSemana = 0;
				
		if(agregador.equals("ANO")){
			iniAgregador = 14;
			fimAgregador = 18;
		}
		
		else if(agregador.equals("MES")){
			iniAgregador = 18;
			fimAgregador = 20;
		}
		
		else if(agregador.equals("SEMANA")){
			iniAgregador = 20;
			fimAgregador = 22;
		}
		
		while(iniAgregador < fimAgregador){
			valor += balde.charAt(iniAgregador);
			iniAgregador++;
		}
		
		if(agregador.equals("MES")){
			return preencheData("ANO", balde)+"-"+valor;
		}
		else if (agregador.equals("SEMANA")){//tratamento para definir as semanas de cada mes
			numeroSemana = 0;
			if(Integer.parseInt(valor)%7 == 0)
				numeroSemana = (int) Math.floor(Integer.parseInt(valor)/7);
			else
				numeroSemana = ((int) Math.floor(Integer.parseInt(valor)/7)) + 1;
			
			return preencheData("MES",balde)+"-"+doisDigitos.format(numeroSemana);
		}
		
		
		return valor;
	}

	private boolean valorValido(String valor) {//checa-se se um atributo de entrada existe:
		int indice = 0;                        //todo valor que contem caracteres diferentes de espaco, 9 ou ponto
		for(indice = 0; indice < valor.length(); indice++){//classifica-se como valido.
			if(valor.charAt(indice) != ' ' && valor.charAt(indice) != '9' && valor.charAt(indice)!= '.')
				return true;
		}
		return false;
	}
}
