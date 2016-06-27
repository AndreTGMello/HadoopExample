package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class RegressionReducer<KEY> extends Reducer<KEY, CompositeWritable, KEY, CompositeWritable> {
	private CompositeWritable resultSet = null;
	private List<CompositeWritable> cache = new ArrayList<CompositeWritable>();
	
	public void reduce(KEY key, Iterable<CompositeWritable> values, Context context) throws IOException, InterruptedException {
		int n = 0;
		double somaX = 0.0;
		double somaY = 0.0;
		double variancia = 0.0;
		double mediaX = 0.0;
		double mediaY = 0.0;
		
		//((Iterator<CompositeWritable>) values).forEachRemaining(cache::add);

		for (CompositeWritable compositeWritable : values) {
			
		}
		for (Iterator<CompositeWritable> iterator = values.iterator(); iterator.hasNext();) {
			double valorY = iterator.next().getEstatisticaY();
			somaY += valorY;
			n++;
			// Cria cache pois nao e possivel iterar novamente
			//cache.add(valorY);
		}
		mediaY = somaY/n;
	}
}
