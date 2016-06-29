import java.io.IOException;

public class Combinador {
	public static void main (String args[]) throws IOException{
		Leitor leitor = new Leitor();
		Escritor escritor = new Escritor();
		leitor.leLista();
		leitor.leHistorico();
	}
}	
