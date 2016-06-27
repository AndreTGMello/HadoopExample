package mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DecimalFormat;
import java.math.RoundingMode;

import org.apache.hadoop.io.Writable;

public class CompositeWritable implements Writable {
    double valor = 0.0;
	double media = 0.0;
    double desvioPadrao = 0.0;
    double variancia = 0.0;
    
    public double getValor() {
		return valor;
	}

	public void setValor(double valor) {
		this.valor = valor;
	}

	public double getMedia() {
		return media;
	}

	public void setMedia(double media) {
		this.media = media;
	}

	public double getDesvioPadrao() {
		return desvioPadrao;
	}

	public void setDesvioPadrao(double desvioPadrao) {
		this.desvioPadrao = desvioPadrao;
	}

	public double getVariancia() {
		return variancia;
	}

	public void setVariancia(double variancia) {
		this.variancia = variancia;
	}

	public CompositeWritable() {}
    
    public CompositeWritable(double valor) {
    	this.valor = valor;
    }

    public CompositeWritable(double media, double desvioPadrao, double variancia) {
    	this.media = media;
        this.desvioPadrao = desvioPadrao;
        this.variancia = variancia;
    } 


    public void readFields(DataInput in) throws IOException {
        valor = in.readInt();
    	media = in.readDouble();
        desvioPadrao = in.readDouble();
        variancia = in.readDouble();
    }

    public void write(DataOutput out) throws IOException {
    	out.writeDouble(valor);
    	out.writeDouble(media);
        out.writeDouble(desvioPadrao);
        out.writeDouble(variancia);
    }

    public void merge(CompositeWritable other) {
    	this.valor += other.valor;
		this.media += other.media;
		this.desvioPadrao += other.desvioPadrao;
		this.variancia += other.variancia;
    }

    @Override
    public String toString() {
    	DecimalFormat df = new DecimalFormat("#.#");
        df.setRoundingMode(RoundingMode.CEILING);
        return df.format(this.media) + "\t" + df.format(this.desvioPadrao) + "\t" + df.format(this.variancia);
    }
}
