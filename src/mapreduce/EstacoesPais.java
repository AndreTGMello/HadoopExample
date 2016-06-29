package mapreduce;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

//Classe que contem todas as estacoes presentes no arquivo <pais_escolhido>.txt
//Segue-se o padrao singleton para a classe nao ser instanciada em outras classes
//mas mesmo assim permitir acesso a mesma.
//Sendo assim, le-se do arquivo uma unica vez, tornando futuras buscas de estacao mais rapidas
public class EstacoesPais {

	public static EstacoesPais getInstance(){
		return instance;
    }
	private static EstacoesPais instance = new EstacoesPais();

	private EstacoesPais(){ //construtor privado impedindo instanciamento direto
    }
	
	private static List<String> listaEstacoes = new LinkedList<String>();
	
	public static void criaListaEstacoes(String pais) throws FileNotFoundException{
		if(!pais.equals("ZZ"))//ZZ significa que nao havera necessidade de criar uma lista, ja que
		{                     //todas as estacoes do mundo serao usadas
			InputStream f = EstacoesPais.class.getResourceAsStream("/"+pais+".txt");//os arquivos das estacoes estao dentro 
			Scanner scan = new Scanner(f);                                          //do proprio arquivo JAR
			while(scan.hasNext()){
				listaEstacoes.add(scan.next());//adiciona-se cada estacao presente no arquivo dentro da lista
			}
			scan.close();
		}
	}
	
	public static List<String> getLista(){
		return listaEstacoes;
	}
	
	public static boolean encontraNaLista(String estacao){//encontra se determinado id de estacao esta presente na lista de
		for(int i = 0; i < listaEstacoes.size(); i++){    //estacoes do pais escolhido pelo usuario
			if(listaEstacoes.get(i).equals(estacao))
				return true;
		}
		return false;
	}
}
