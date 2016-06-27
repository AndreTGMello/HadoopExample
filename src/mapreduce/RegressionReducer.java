package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class RegressionReducer<KEY> extends Reducer<KEY, CompositeWritable, KEY, CompositeWritable> {
	private CompositeWritable resultSet = null;
	private List<CompositeWritable> cache = new ArrayList<CompositeWritable>();
	
	public void reduce(KEY key, Iterable<CompositeWritable> values, Context context) throws IOException, InterruptedException {
		int n = 0;
		int somaX = 0;
		double somaY = 0.0;
		double mediaX = 0.0;
		double mediaY = 0.0;
		double b = 0.0;
		double a = 0.0;
		double bNumerador = 0.0;
		double bDenominador = 0.0;
		double xMin = Double.MAX_VALUE;
		double xMax = 0.0;
		
		for (Iterator<CompositeWritable> iterator = values.iterator(); iterator.hasNext();) {
			CompositeWritable aux = iterator.next();
			
			DoubleWritable valorYWritable = aux.getEstatisticaY();
			double valorY = valorYWritable.get();
			
			System.out.println("\n\tOY"+valorY+"\n");
			somaY += valorY;
			
			Text valorXWritable = aux.getAgrupadorX();
			int valorX = Integer.parseInt(valorXWritable.toString());
			
			System.out.println("\n\tOX"+valorX+"\n");
			
			somaX += valorX;
			n++;
			if(valorX < xMin){
				xMin = valorX;
			}
			if(valorX > xMax){
				xMax = valorX;
			}
			// Cria cache pois nao e possivel iterar novamente
			cache.add(aux);
		}
		mediaY = somaY/n;
		mediaX = somaX/n;
		
		for (Iterator<CompositeWritable> iterator = cache.iterator(); iterator.hasNext();) {
			CompositeWritable aux = (CompositeWritable) iterator.next();
			
			DoubleWritable valorYWritable = aux.getEstatisticaY();
			double valorY = valorYWritable.get();
			
			Text valorXWritable = aux.getAgrupadorX();
			int valorX = Integer.parseInt(valorXWritable.toString());
			
			bNumerador += (valorX*(valorY-mediaY));
			bDenominador += (valorX*(valorX-mediaX));
		}
		b = bNumerador/bDenominador;
		a = mediaY - b*mediaX;
		
		DoubleWritable aWritable = new DoubleWritable();
		aWritable.set(a);
		
		DoubleWritable bWritable = new DoubleWritable();
		bWritable.set(b);
		
		DoubleWritable xMinWritable = new DoubleWritable();
		xMinWritable.set(xMin);
		
		DoubleWritable xMaxWritable = new DoubleWritable();
		xMaxWritable.set(xMax);
		
		resultSet = new CompositeWritable(aWritable, bWritable, xMinWritable, xMaxWritable);
		context.write(key, resultSet);
	}
}
