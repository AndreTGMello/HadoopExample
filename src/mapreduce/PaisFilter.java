package mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class PaisFilter implements PathFilter {

	public boolean accept(Path path) {
		String nome = path.getName();
		if (nome.length() == 4)
			return true;
		EstacoesPais lista = EstacoesPais.getInstance();
		String[] ids = nome.split("-");
		String id = "";
		if(!ids[0].equals("999999"))
			id = ids[0];
		else
			id = ids[1];
		
		if(lista.encontraNaLista(id))
			return true;
		
		return false;
	}

}
