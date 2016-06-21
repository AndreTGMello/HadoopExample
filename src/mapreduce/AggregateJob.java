package mapreduce;

import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AggregateJob extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		conf.set("agregador", args[3]);
		conf.set("dado", args[4]);
		
		//JobConf job = new JobConf(conf);
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(getClass());
	    job.setJobName(getClass().getSimpleName());
	    
	    //FileInputFormat.addInputPath(job, new Path(args[0]+"/"+args[5])); //new Path(args[0]+"/*nome_da_pasta" para que arquivos dentro de pastas tambem sejam lidos
	    int anoIni = 0;
	    int anoFim = Integer.parseInt(args[6]);
	    for(anoIni = Integer.parseInt(args[5]); anoIni <= anoFim; anoIni++){
	    	MultipleInputs.addInputPath(job, new Path(args[0]+"/"+anoIni), SingleInputFormat.class, MediaMapper.class);
	    }
	    FileOutputFormat.setOutputPath(job, new Path(args[1]+"/"+args[2]));
	    
	    job.setMapperClass(MediaMapper.class);
	    job.setCombinerClass(MediaReducer.class);
	    job.setReducerClass(MediaReducer.class);

	    /*
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(CompositeWritable.class);
	    */
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(CompositeWritable.class);
	    
	    job.setInputFormatClass(SingleInputFormat.class);
	    
	    return job.waitForCompletion(true) ? 0 : 1;
	}
	
		public static void main(String[] args) throws Exception {
			Scanner scan = new Scanner(System.in);
			
			String dado = null;
			String anoIni = null;
			String anoFim = null;
			String agregador = null; 
			String raizSaida = null;
			String pastaSaida = null;
			String entrada = null;
			String[] params = new String[7];
			
			System.out.println("Digite a pasta onde se encontram os dados: ");
			entrada = scan.next();
			params[0] = entrada;
			
			System.out.println("Digite a raiz da pasta onde deseja armazenar os dados: ");
			raizSaida = scan.next();
			params[1] = raizSaida;
			
			while(true){
				System.out.println("Digite qual dado deseja analisar. "
						+ "\nOpcoes: "
						+ "\nMedTemperatura = Temperatura media"
						+ "\nMedCondensassao = Ponto medio de condensassao da agua"
						+ "\nMedMar = Nivel do mar medio"
						+ "\nMedPressao = Pressao media"
						+ "\nMedVento = Velocidade do vento media"
						+ "\nMaxVento = Velocidade maxima do vento"
						+ "\nMaxRajada = Velocidade maxima das rajadas de vento"
						+ "\nMaxTemperatura = Maximas da temperatura"
						+ "\nMinTemperatura = Minimas da temperatura"
						+ "\nPrecipitacao = Quantidade de precipitacao"
						+ "\nNeve = Profundidade da neve"
						+ "\n");
				dado = scan.next();
				
				System.out.println("Digite sobre qual intervalo de anos deseja realizar a operacao: ");
				System.out.println("Inicio: ");
				anoIni = scan.next();
				System.out.println("Fim: ");
				anoFim = scan.next();
				
				System.out.println("Digite como deseja agregar os dados"
						+ "\nOpcoes: "
						+ "\nAno"
						+ "\nMes"
						+ "\nSemana: ");
				agregador = scan.next();
				
				System.out.println("Digite o nome do arquivo de saida (nao repita nomes): ");
				pastaSaida = scan.next();
				
				params[2] = pastaSaida;
				params[3] = agregador;
				params[4] = dado;
				params[5] = anoIni;
				params[6] = anoFim;
				
				
				// Roda run() com a lista de argumentos passados na execucao do jar
				// + argumentos obtidos interativamente atraves das perguntas acima.
			    int rc = ToolRunner.run(new AggregateJob(), params);
			    System.exit(rc);
			}
			
		}
}
