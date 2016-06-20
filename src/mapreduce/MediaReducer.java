package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
 
public class MediaReducer<KEY> extends Reducer<KEY, DoubleWritable,KEY,DoubleWritable> {
 
  private DoubleWritable result = new DoubleWritable();
  
  public void reduce(KEY key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
    double sum = 0;
    int contador = 0;
    for (DoubleWritable val : values) {
      sum += val.get();
      contador++;
    }
    result.set(sum/contador);
    context.write(key, result);
  }
 
}
