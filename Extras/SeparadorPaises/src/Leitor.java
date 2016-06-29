import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Scanner;

public class Leitor {
	public static Map<String,LinkedList<String>> numEst = new HashMap<String,LinkedList<String>>();
	LinkedList<String> listaSiglas = new LinkedList<String>();
	public void leLista() throws IOException {
		String pais = "";
		String sigla = "";
		Escritor escritor = new Escritor();
		File f = new File ("/home/tina/Documentos/country-list.txt");
		Scanner scan = new Scanner(f);
		while(scan.hasNextLine()){
			pais = scan.nextLine();
			sigla = String.valueOf(pais.charAt(0)) + String.valueOf(pais.charAt(1));
			numEst.put(sigla, new LinkedList<String>());
			listaSiglas.add(sigla);
			escritor.criaArquivo(sigla);
		}	
		scan.close();
	}

	public void leHistorico() throws IOException {
		String linha = "";
		String estacao = "";
		String sigla = "";
		Escritor escritor = new Escritor();
		File f = new File ("/home/tina/Documentos/isd-history.txt");
		Scanner scan = new Scanner(f);
		while(scan.hasNextLine()){
			linha = "";
			estacao = "";
			sigla = "";
			linha = scan.nextLine();
			for(int i = 0; i < 6; i ++) //0 a 5
				estacao += String.valueOf(linha.charAt(i));
			if(estacao.equals("999999")){
				estacao = "";
				for(int j = 7; j < 12; j++)// 7 a 11
					estacao += String.valueOf(linha.charAt(j));
			}
			sigla = String.valueOf(linha.charAt(43)) + String.valueOf(linha.charAt(44));
			if(!sigla.equals("  ")){
				if(numEst.get(sigla) != null){
					numEst.get(sigla).add(estacao);
				}	
			}	
		}
		escritor.escreve(numEst, listaSiglas);
		escritor.apagaVazio(listaSiglas);
		scan.close();
	}
}
