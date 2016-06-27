package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
 
public class StatisticsReducer<KEY> extends Reducer<KEY, DoubleWritable, KEY, CompositeWritable> {
 
	private CompositeWritable resultSet = null;
	private List<Double> cache = new ArrayList<Double>();
  
	public void reduce(KEY key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		int n = 0;
		double soma = 0.0;
		double variancia = 0.0;
		double media = 0.0;
		double desvioPadrao = 0.0;
		
		for (Iterator<DoubleWritable> iterator = values.iterator(); iterator.hasNext();) {
			double valor = iterator.next().get();
			soma += valor;
			n++;
			// Cria cache pois nao e possivel iterar novamente
			cache.add(valor);
		}
		media = soma/n;
		
		// Utiliza cache pare iterar sobre os dados mais uma vez
		for (Iterator<Double> iterator = cache.iterator(); iterator.hasNext();) {
			variancia += Math.pow((iterator.next()-media), 2);
		}
		variancia = variancia/(n-1);
		desvioPadrao = Math.sqrt(variancia);
		
		DoubleWritable mediaWritable = new DoubleWritable();
		mediaWritable.set(media);
		System.out.println("Media: "+mediaWritable);
		
		DoubleWritable desvioPadraoWritable = new DoubleWritable();
		desvioPadraoWritable.set(desvioPadrao);
		System.out.println("Desvio padrao: "+desvioPadraoWritable);
		
		DoubleWritable varianciaWritable = new DoubleWritable();
		varianciaWritable.set(variancia);
		System.out.println("Variancia "+varianciaWritable);
		
		resultSet = new CompositeWritable(mediaWritable, desvioPadraoWritable, varianciaWritable);
		context.write(key, resultSet);
  }
 
}
