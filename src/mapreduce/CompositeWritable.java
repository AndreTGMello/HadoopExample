package mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.math.RoundingMode;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class CompositeWritable implements Writable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private DoubleWritable valor = null;
	private DoubleWritable media = null;
	private DoubleWritable desvioPadrao = null;
	private DoubleWritable variancia = null;
	private Text agrupadorX = null;
	private DoubleWritable estatisticaY = null;
	private DoubleWritable xMin = null;
	private DoubleWritable xMax = null;
	private DoubleWritable a = null;
	private DoubleWritable b = null;
	// Flag false = escreve estatisticas
	// Flag true = escreve reta


	public CompositeWritable() {}

	public CompositeWritable(DoubleWritable valor) {
		this.valor = valor;
	}

	public CompositeWritable(DoubleWritable media, DoubleWritable desvioPadrao, DoubleWritable variancia) {
		this.media = media;
		this.desvioPadrao = desvioPadrao;
		this.variancia = variancia;
	} 

	public CompositeWritable(Text x, DoubleWritable y) {
		this.estatisticaY = y;
		this.agrupadorX = x;
	} 

	public CompositeWritable(DoubleWritable a, DoubleWritable b, DoubleWritable xMin, DoubleWritable xMax) {
		this.a = a;
		this.b = b;
		this.xMin = xMin;
		this.xMax = xMax;
	}
	
	
	public void readFields(DataInput in) throws IOException {
		valor.readFields(in);
		media.readFields(in);
		desvioPadrao.readFields(in);
		variancia.readFields(in);
		
		agrupadorX.readFields(in);
		estatisticaY.readFields(in);
		xMin.readFields(in);
		xMax.readFields(in);
		a.readFields(in);
		b.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		valor.write(out);
		media.write(out);
		desvioPadrao.write(out);
		variancia.write(out);
		
		agrupadorX.write(out);
		estatisticaY.write(out);
		xMin.write(out);
		xMax.write(out);
		a.write(out);
		b.write(out);
	}
	
	/*
	public void merge(CompositeWritable other) {
		this.valor += other.valor;
		this.media += other.media;
		this.desvioPadrao += other.desvioPadrao;
		this.variancia += other.variancia;
	}
	*/

	@Override
	public String toString() {
		if(a != null & b != null){
			return this.a + "\t" + this.b + "\t" + this.xMin + "\t" + this.xMax; 
		}else{
			DecimalFormat df = new DecimalFormat("#.#");
			df.setRoundingMode(RoundingMode.CEILING);
			return (df.format(this.media) + "\t" + df.format(this.desvioPadrao) + "\t" + df.format(this.variancia)).replace(',', '.');
		}

	}
}
