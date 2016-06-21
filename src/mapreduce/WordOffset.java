package mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


class WordOffset implements WritableComparable<Object> {

    long offset;
    String fileName;

    public void readFields(DataInput in) throws IOException {
      this.offset = in.readLong();
      this.fileName = Text.readString(in);
    }

    public void write(DataOutput out) throws IOException {
      out.writeLong(offset);
      Text.writeString(out, fileName);
    }

    public int compareTo(Object o) {
      WordOffset that = (WordOffset)o;

      int f = this.fileName.compareTo(that.fileName);
      if(f == 0) {
        return (int)Math.signum((double)(this.offset - that.offset));
      }
      return f;
    }
    @Override
    public boolean equals(Object obj) {
      if(obj instanceof WordOffset)
        return this.compareTo(obj) == 0;
      return false;
    }
    @Override
    public int hashCode() {
      assert false : "hashCode not designed";
      return 42; //an arbitrary constant
    }
  }