package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
 
public class MediaReducer<KEY> extends Reducer<KEY, DoubleWritable, KEY, CompositeWritable> {
 
	private CompositeWritable resultSet = null;
  
	public void reduce(KEY key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		int n = 0;
		double soma = 0.0;
		double variancia = 0.0;
		double media = 0.0;
		double desvioPadrao = 0.0;
/*
		for (CompositeWritable compWrite : values) {
			compWrite.merge(compWrite);
			n++;
		}
*/
		System.out.println("Reducer1: "+n+","+soma+","+media+","+variancia+","+desvioPadrao);
		
		for (DoubleWritable cw : values) {
			System.out.println("valor: "+cw.get());
			System.out.println("Soma1: "+soma);
			soma += cw.get();
			System.out.println("Soma2: "+soma);
			n++;
		}
		media = soma/n;
		
		for (DoubleWritable cw : values) {
			variancia += Math.pow((cw.get()-media), 2);
		}
		variancia = variancia/(n-1);
		desvioPadrao = Math.sqrt(variancia);
		
		System.out.println("Reducer2: "+n+","+soma+","+media+","+variancia+","+desvioPadrao);
		
		resultSet = new CompositeWritable(media, variancia, desvioPadrao);
		context.write(key, resultSet);
  }
 
}
