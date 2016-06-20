package mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CompositeWritable implements Writable {
    double media = 0;
    double desvioPadrao = 0;
    double variancia = 0;

    public CompositeWritable() {}

    public CompositeWritable(double media, double desvioPadrao, double variancia) {
        this.media = media;
        this.desvioPadrao = desvioPadrao;
        this.variancia = variancia;
    }

    public void readFields(DataInput in) throws IOException {
        media = in.readDouble();
        desvioPadrao = in.readDouble();
        variancia = in.readDouble();
    }

    public void write(DataOutput out) throws IOException {
        out.writeDouble(media);
        out.writeDouble(desvioPadrao);
        out.writeDouble(variancia);
    }

    public void merge(CompositeWritable other) {
        this.media += other.media;
        this.desvioPadrao += other.desvioPadrao;
        this.variancia += other.variancia;
    }

    @Override
    public String toString() {
        return this.media + "\t" + this.desvioPadrao + "\t" + this.variancia;
    }
}
