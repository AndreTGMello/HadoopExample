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

public class CompositeWritable implements Writable, Comparable<CompositeWritable>{
	private DoubleWritable valor = new DoubleWritable();
	private DoubleWritable media = new DoubleWritable();
	private DoubleWritable desvioPadrao = new DoubleWritable();
	private DoubleWritable variancia = new DoubleWritable();
	private Text agrupadorX = new Text();
	private DoubleWritable estatisticaY = new DoubleWritable();
	private DoubleWritable xMin = new DoubleWritable();
	private DoubleWritable xMax = new DoubleWritable();
	private DoubleWritable a = new DoubleWritable();
	private DoubleWritable b = new DoubleWritable();
	
	
	public DoubleWritable getValor() {
		return valor;
	}

	public void setValor(DoubleWritable valor) {
		this.valor = valor;
	}

	public DoubleWritable getMedia() {
		return media;
	}

	public void setMedia(DoubleWritable media) {
		this.media = media;
	}

	public DoubleWritable getDesvioPadrao() {
		return desvioPadrao;
	}

	public void setDesvioPadrao(DoubleWritable desvioPadrao) {
		this.desvioPadrao = desvioPadrao;
	}

	public DoubleWritable getVariancia() {
		return variancia;
	}

	public void setVariancia(DoubleWritable variancia) {
		this.variancia = variancia;
	}

	public Text getAgrupadorX() {
		return agrupadorX;
	}

	public void setAgrupadorX(Text agrupadorX) {
		this.agrupadorX = agrupadorX;
	}

	public DoubleWritable getEstatisticaY() {
		return estatisticaY;
	}

	public void setEstatisticaY(DoubleWritable estatisticaY) {
		this.estatisticaY = estatisticaY;
	}

	public DoubleWritable getxMin() {
		return xMin;
	}

	public void setxMin(DoubleWritable xMin) {
		this.xMin = xMin;
	}

	public DoubleWritable getxMax() {
		return xMax;
	}

	public void setxMax(DoubleWritable xMax) {
		this.xMax = xMax;
	}

	public DoubleWritable getA() {
		return a;
	}

	public void setA(DoubleWritable a) {
		this.a = a;
	}

	public DoubleWritable getB() {
		return b;
	}

	public void setB(DoubleWritable b) {
		this.b = b;
	}

	public CompositeWritable() {}

	public CompositeWritable(DoubleWritable valor) {
		this.valor = valor;
	}

	public CompositeWritable(DoubleWritable media, DoubleWritable desvioPadrao, DoubleWritable variancia) {
		this.media = media;
		this.desvioPadrao = desvioPadrao;
		this.variancia = variancia;
		System.out.println("Media: "+media+". this.media: "+this.media.get()+"\n"
				+ "Desvio padrao: "+desvioPadrao+". this.desvioPadrao: "+this.desvioPadrao.get()+"\n"
						+ "Variancia: "+variancia+". this.variancia: "+this.variancia.get()+"\n");
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
		if(a.get()!=0.0 && b.get()!=0.0){
			double aDouble = this.a.get();
			double bDouble = this.b.get();
			double xMinDouble = this.xMin.get();
			double xMaxDouble = this.xMax.get();
			return aDouble + "\t" + bDouble + "\t" + xMinDouble + "\t" + xMaxDouble; 
		}else{
			DecimalFormat df = new DecimalFormat("#.#");
			df.setRoundingMode(RoundingMode.CEILING);
			double mediaDouble = this.media.get();
			System.out.println("Composite media "+mediaDouble);
			double desvioPadraoDouble = this.desvioPadrao.get();
			System.out.println("Composite desvio padrao"+desvioPadraoDouble);
			double varianciaDouble = this.variancia.get();
			System.out.println("Composite variancia "+varianciaDouble);
			return (df.format(mediaDouble) + "\t" + df.format(desvioPadraoDouble) + "\t" + df.format(varianciaDouble)).replace(',', '.');
		}

	}

	public int compareTo(CompositeWritable o) {
		int diff = Integer.MAX_VALUE;
		int esseX = 0;
		int outroX = 0;
		String esseXString = this.agrupadorX.toString();
		String outroXString = o.getAgrupadorX().toString();
		try {
			esseX = Integer.parseInt(esseXString);
			outroX = Integer.parseInt(outroXString);
			diff = esseX - outroX;
		} catch (Exception e) {
			e.printStackTrace();
		}
		// Parse Int funcionara para mes e ano:
		if(diff!=Integer.MAX_VALUE){
			return diff;
		}
		// Caso especial: semanas
		else{
			String[] esseXArray = esseXString.split("-");
			String[] outroXArray = outroXString.split("-");
			int diffAno = Integer.parseInt(esseXArray[0]) - Integer.parseInt(outroXArray[0]);
			int diffMes = Integer.parseInt(esseXArray[1]) - Integer.parseInt(outroXArray[1]);
			int diffSemana = Integer.parseInt(esseXArray[2]) - Integer.parseInt(outroXArray[2]);
			
			if(diffAno!=0){
				return diffAno;
			}else if(diffMes!=0){
				return diffMes;
			}else if(diffSemana!=0){
				return diffSemana;
			}
		}
		return 0;
	}
}
