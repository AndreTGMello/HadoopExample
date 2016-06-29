import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.LinkedList;
import java.util.Map;

public class Escritor {
	BufferedWriter bw = null;
	public void criaArquivo(String pais) throws IOException {
		Writer writer = new FileWriter("/home/tina/Documentos/Paises/"+pais+".txt");
		writer.close();
	}

	public void escreve(Map<String, LinkedList<String>> numEst, LinkedList<String> listaSiglas) throws IOException {
		for(int i = 0; i < listaSiglas.size(); i ++){
			LinkedList<String> numerosEstacao = new LinkedList<String>();
			FileWriter file = new FileWriter("/home/tina/Documentos/Paises/"+listaSiglas.get(i)+".txt",true);
			bw = new BufferedWriter(file);
			numerosEstacao = numEst.get(listaSiglas.get(i));
			for(int j = 0; j < numerosEstacao.size(); j++){
				//System.out.println(listaSiglas.get(i) + numerosEstacao.get(j));
				bw.write(numerosEstacao.get(j));
				bw.newLine();
			}
			bw.close();
		}
		
	}

	public void apagaVazio(LinkedList<String> listaSiglas) {
		for(int i = 0; i < listaSiglas.size(); i ++){
			File file = new File("/home/tina/Documentos/Paises/"+listaSiglas.get(i)+".txt");
			if(file.length() == 0){
				file.delete();
				System.out.println("Deletando "+listaSiglas.get(i));
			}
		}	
	}
}
