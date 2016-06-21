package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
 
public class MediaReducer<KEY> extends Reducer<KEY, CompositeWritable, KEY, CompositeWritable> {
 
	private CompositeWritable resultSet = null;
  
	public void reduce(KEY key, Iterable<CompositeWritable> values, Context context) throws IOException, InterruptedException {
		int n = 0;
		double soma = 0;
		double variancia = 0;
		double media = 0;
		double desvioPadrao = 0;
/*
		for (CompositeWritable compWrite : values) {
			compWrite.merge(compWrite);
			n++;
		}
*/
		for (CompositeWritable cw : values) {
			soma += cw.getValor();
			n++;
		}
		media = soma/n;
		
		for (CompositeWritable cw : values) {
			variancia += Math.pow((cw.getValor()-media), 2);
		}
		variancia = variancia/(n-1);
		desvioPadrao = Math.sqrt(variancia);
		
		resultSet = new CompositeWritable(media, variancia, desvioPadrao);
		context.write(key, resultSet);
  }
 
}
