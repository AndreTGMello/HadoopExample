package mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
//Classe responsavel por filtrar os nomes de arquivo de acordo com a lista de paises
public class PaisFilter implements PathFilter {

	public boolean accept(Path path) {
		String nome = path.getName();
		if (nome.length() == 4)//caso o arquivo seja uma pasta de ano
			return true;       //ele tera tamanho 4, no formato 19XX ou 20XX, logo devera ser aceito e entrar
		EstacoesPais lista = EstacoesPais.getInstance();
		String[] ids = nome.split("-");//o nome do arquivo contem os dois tipos de id para uma estacao
		String id = "";
		if(!ids[0].equals("999999"))
			id = ids[0];
		else            //caso o primeiro id seja inexistente
			id = ids[1];//obrigatoriamente o segundo existe e deve ser usado
		
		if(lista.encontraNaLista(id))//se a estacao existe na lista do pais escolhido
			return true;             //entao deve-se aceitar o arquivo
		
		return false;                //caso contrario, ele sera filtrado
	}

}
