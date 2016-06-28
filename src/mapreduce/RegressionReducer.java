package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
		int xMin = Integer.MAX_VALUE;
		int xMax = 0;
		
		
		for (Iterator<CompositeWritable> iterator = values.iterator(); iterator.hasNext();) {
			CompositeWritable temp = iterator.next();
			
			Text agrupadorX = new Text();
			agrupadorX.set(temp.getAgrupadorX().toString());
			
			DoubleWritable estatisticaY = new DoubleWritable();
			estatisticaY.set(temp.getEstatisticaY().get());;
			
			CompositeWritable newTemp = new CompositeWritable(agrupadorX, estatisticaY);
			System.out.println("Inserindo: ");
			System.out.println(newTemp.getAgrupadorX().toString());
			System.out.println(newTemp.getEstatisticaY().get());
			cache.add(newTemp);
		}
		
		System.out.println("FIM1");
		for (int i = 0; i < cache.size(); i++) {
			System.out.println("Inseridos: ");
			System.out.println(cache.get(i).getAgrupadorX().toString());
			System.out.println(cache.get(i).getEstatisticaY().get());
		}
		System.out.println("FIM2");
		
		// Utiliza o metodo compareTo implementado em CompositeWritable
		Collections.sort(cache);
		
		for (int i = 0; i < cache.size(); i++) {
			System.out.println("Pos Sort: ");
			System.out.println(cache.get(i).getAgrupadorX().toString());
			System.out.println(cache.get(i).getEstatisticaY().get());
		}
		System.out.println("FIM3");
		
		for (int i = 0; i < cache.size(); i++) {
			CompositeWritable cw = cache.get(i);
			DoubleWritable valorYWritable = cw.getEstatisticaY();
			double valorY = valorYWritable.get();
			somaY += valorY;
			
			int valorX = i;
			somaX += valorX;
			
			System.out.println("\tValor Y: "+valorY+"\n");
			System.out.println("\tValor X: "+valorX+"\n");
			System.out.println("\tValor X real: "+cw.getAgrupadorX().toString()+"\n");
			System.out.println("\tSomaX: "+somaX+"\n");
			System.out.println("\tSomaY: "+somaY+"\n");
			
			n++;
			if(valorX < xMin){
				xMin = valorX;
			}
			if(valorX > xMax){
				xMax = valorX;
			}
		}
		mediaY = somaY/n;
		mediaX = somaX/n;
		System.out.println("MediaX: "+mediaX+". MediaY: "+mediaY);
		
		for (int i = 0; i < cache.size(); i++) {
			CompositeWritable cw = cache.get(i);
			
			DoubleWritable valorYWritable = cw.getEstatisticaY();
			double valorY = valorYWritable.get();
			
			int valorX = i;
			
			bNumerador += (valorX*(valorY-mediaY));
			bDenominador += (valorX*(valorX-mediaX));
			
			System.out.println("\tValor Y: "+valorY+"\n");			
			System.out.println("\tValor X: "+valorX+"\n");
			System.out.println("\tValor X real: "+cw.getAgrupadorX().toString()+"\n");
			System.out.println("bNumerador: "+bNumerador);
			System.out.println("bDenominador: "+bDenominador);
			
		}
		b = bNumerador/bDenominador;
		a = mediaY - b*mediaX;
		
		DoubleWritable aWritable = new DoubleWritable();
		aWritable.set(a);
		
		DoubleWritable bWritable = new DoubleWritable();
		bWritable.set(b);
		
		IntWritable xMinWritable = new IntWritable();
		xMinWritable.set(xMin);
		
		IntWritable xMaxWritable = new IntWritable();
		xMaxWritable.set(xMax);
		
		resultSet = new CompositeWritable(aWritable, bWritable, xMinWritable, xMaxWritable);
		context.write(key, resultSet);
	}
}
