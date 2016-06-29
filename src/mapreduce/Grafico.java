package mapreduce;

import java.awt.Color;
import java.awt.Font;
import java.io.File;
import java.io.FileNotFoundException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Scanner;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.CategoryLabelPosition;
import org.jfree.chart.axis.CategoryLabelPositions;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.category.LineAndShapeRenderer;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;

public class Grafico extends ApplicationFrame {
	public static String caminhoPrimeiraSaida = "";//caminho para o output gerado pela primeira tarefa de mapreduce
	public static String caminhoSegundaSaida = "";//caminho para o output gerado pela segunda tarefa de mapreduce: a regressao
	public static String estatistica = "";//estatistica escolhida pelo usuario(media, desviopadrao ou variancia)
	public static ArrayList<String> categorias = new ArrayList<String>();//valores dos labels do eixo x
    public Grafico(final String title) throws FileNotFoundException {
        super(title);
        final JFreeChart chart = ChartFactory.createLineChart("Grafico", "Periodo", "Estatistica", createDataset(), PlotOrientation.VERTICAL, true, true, false);
        CategoryPlot plot = (CategoryPlot) chart.getPlot();
        LineAndShapeRenderer renderer = (LineAndShapeRenderer) plot.getRenderer();
        renderer.setSeriesShapesVisible(0, true);//coloca os pontos das retas de modo visivel
        renderer.setSeriesShapesVisible(1, true);
        CategoryAxis axis = chart.getCategoryPlot().getDomainAxis();
        int baldeCategoria = 1;
        double proporcaoD = (categorias.size()+10)/41; //determinando quantos labels do eixo X podem
        int proporcao = (int) proporcaoD;              //aparecer de forma a continuar sempre legivel
        if(proporcao == 0)//para evitar uma divisao por 0
        	proporcao = 1;
        int indiceCategoria = 0;
        for(; indiceCategoria < categorias.size(); indiceCategoria++)
        {   //o modo encontrado de esconder um label, foi pinta-lo de branco e colocar tamanho minimo
        	axis.setTickLabelPaint(categorias.get(indiceCategoria), Color.white);
        	if(baldeCategoria%proporcao == 0)
        	{
        		axis.setTickLabelPaint(categorias.get(indiceCategoria), Color.black);
        	}
        	else
        		axis.setTickLabelFont(categorias.get(indiceCategoria),new Font("Helvetica", Font.PLAIN, 0));
        	baldeCategoria++;
        }
        for(indiceCategoria = 0; indiceCategoria < 10; indiceCategoria++)//realiza o mesmo procedimento para os valores de p0 a p9 
        {                                                                //que sao previsoes de valores alem dos escolhidos pelo usuario
        	axis.setTickLabelPaint("P"+indiceCategoria, Color.white);
        	if(baldeCategoria%proporcao == 0)
        	{
        		axis.setTickLabelPaint("P"+indiceCategoria, Color.black);
        	}
        	else
        		axis.setTickLabelFont("P"+indiceCategoria,new Font("Helvetica", Font.PLAIN, 0));
        	baldeCategoria++;
        }
        axis.setCategoryLabelPositions(CategoryLabelPositions.UP_90);//para melhor visibilidade, coloca-se verticalmente os labels do eixo x
        final ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setPreferredSize(new java.awt.Dimension(500, 270));
        setContentPane(chartPanel);
    }
    
    private DefaultCategoryDataset createDataset() throws FileNotFoundException {
    	final DefaultCategoryDataset dataset = new DefaultCategoryDataset();
    	String moldeArquivo = "part-r-00000";
    	File f = new File(caminhoSegundaSaida+"/"+moldeArquivo); //abre o output gerado pela regressao para
    	Scanner scan = new Scanner(f);                           //pegar os valores necessarios para tracar a
    	double a = 0.0;                                          //reta da previsao dinamicamente
    	double b = 0.0;
    	double resultadofuncao = 0.0;
    	scan.next();
    	a = Double.parseDouble(scan.next());
    	b = Double.parseDouble(scan.next());
    	scan.close();
    	f = new File(caminhoPrimeiraSaida+"/"+moldeArquivo);//depois, pega-se o primeiro output para colocar
    	int indiceRegressao = 0;                            //na reta os valores da estatistica escolhida na primeira etapa
    	while(f.exists())
    	{
    		scan = new Scanner(f);
        	String x = "";
        	double y = 0;
        	int indiceEstatistica = 0;
        	if(estatistica.equals("MEDIA"))
        		indiceEstatistica = 1;
        	else if (estatistica.equals("DESVIOPADRAO"))
        		indiceEstatistica = 2;
        	else if(estatistica.equals("VARIANCIA"))
        		indiceEstatistica = 3;       	
        		
        	while(scan.hasNext()){
        		
        		x = scan.next();
        		categorias.add(x);
        		int z = 0;
        		for(z = 0; z < indiceEstatistica; z++)
        		{
        			y = Double.parseDouble(scan.next());
        		}
        		for(; z < 3; z++)
        		{
        			scan.next();
        		}
        		dataset.addValue(y, "valor estatistica", x);//adiciona valores ao grafico
        		resultadofuncao = a + b*indiceRegressao;
        		dataset.addValue(resultadofuncao, "previsao", x);
        		indiceRegressao++;
        		
        	}
        	for(int h = 0; h < 10; h++){//realiza 10 previsoes para alem do ano maximo escolhido pelo usuario
        		resultadofuncao = a + b*indiceRegressao;
            	dataset.addValue(resultadofuncao, "previsao", "P"+Integer.toString(h));
            	indiceRegressao++;
        	}
        	DecimalFormat doisDigitos = new DecimalFormat("00000");//permite que, caso tenha mais
        	int numArquivo = Integer.parseInt(moldeArquivo.split("-")[2]);//do que um output
        	numArquivo++;                                         //consiga-se ler todos os
        	moldeArquivo = "part-r-"+doisDigitos.format(numArquivo);//arquivos para gerar
        	f = new File(caminhoPrimeiraSaida+"/"+moldeArquivo);  //o grafico corretamente
        	
        	
    	}
        
    	scan.close();
        return dataset;
    }
    
    public static void criaGrafico(String argUm, String argDois, String argOito) {
    	//argUm: raiz da pasta de saida escolhida pelo usuario
    	//argDois: nome do arquivo de saida escolhido pelo usuario
    	//argOito: estatistica escolhida pelo usuario para a regressao
        caminhoPrimeiraSaida = argUm + "/" + argDois;
        estatistica = argOito;
        caminhoSegundaSaida = caminhoPrimeiraSaida + "/regressao" + estatistica;
    	Grafico demo;
		try {
			demo = new Grafico("Grafico");
			demo.pack();
	        RefineryUtilities.centerFrameOnScreen(demo);
	        demo.setVisible(true);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
    }
}
