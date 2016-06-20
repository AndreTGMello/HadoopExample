package mapreduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
 
public class MediaMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	private Text word = new Text();
	private DoubleWritable count = new DoubleWritable();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String balde = value.toString();
		if(balde.charAt(0) != 'S'){
			String texto = "";
			int i = 0;
			while(i < 6){
				texto += balde.charAt(i);
				i++;
			}	
			word.set(texto);
			
			i= 24;
			texto = "";
			while(i < 30){
				if(balde.charAt(i) != ' '){
					texto += balde.charAt(i);
				}
				i++;
			}	
			count.set(Double.parseDouble(texto));
			try {
				context.write(word, count);
			} 
			catch (NumberFormatException e) {
				
			}
		}
	}
}