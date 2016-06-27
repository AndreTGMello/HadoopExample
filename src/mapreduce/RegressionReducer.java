package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class RegressionReducer<KEY> extends Reducer<KEY, DoubleWritable, KEY, CompositeWritable> {
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
	}
}
