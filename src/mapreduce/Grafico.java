package mapreduce;

import java.awt.Color;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;

public class Grafico extends ApplicationFrame {
	public static String caminhoPrimeiraSaida = "";
	public static String caminhoSegundaSaida = "";
	public static String estatistica = "";
    public Grafico(final String title) throws FileNotFoundException {
        super(title);
        final JFreeChart chart = ChartFactory.createLineChart("Grafico", "Periodo", "Estatistica", createDataset(), PlotOrientation.VERTICAL, true, true, false);
        final ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setPreferredSize(new java.awt.Dimension(500, 270));
        setContentPane(chartPanel);
    }
    
    private DefaultCategoryDataset createDataset() throws FileNotFoundException {
    	final DefaultCategoryDataset dataset = new DefaultCategoryDataset();
    	String moldeArquivo = "part-r-00000"; //permitir mais depois
    	File f = new File(caminhoSegundaSaida+"/"+moldeArquivo);
    	Scanner scan = new Scanner(f);
    	double a = 0.0;
    	double b = 0.0;
    	int anoIniBalde = 0; 
    	//String anoIni = "";
    	double resultadofuncao = 0.0;
    	scan.next();
    	a = Double.parseDouble(scan.next());
    	b = Double.parseDouble(scan.next());
    	anoIniBalde = (int) (Double.parseDouble(scan.next()));//para tirar a casa decimal do ano
    	scan.close();
        f = new File(caminhoPrimeiraSaida+"/"+moldeArquivo);
        scan = new Scanner(f);
    	String x = "";
    	double y = 0;
    	int indiceEstatistica = 0;
    	if(estatistica.equals("MEDIA"))
    		indiceEstatistica = 1;
    	else if (estatistica.equals("DESVIOPADRAO"))
    		indiceEstatistica = 2;
    	else if(estatistica.equals("VARIANCIA"))//CONFIRMAR RS
    		indiceEstatistica = 3;
    	int indiceRegressao = 0;
    		
    	while(scan.hasNext()){//faz primeira reta
    		
    		x = scan.next();
    		int z = 0;
    		for(z = 0; z < indiceEstatistica; z++)
    		{
    			y = Double.parseDouble(scan.next());
    		}
    		for(; z < 3; z++)
    		{
    			scan.next();
    		}
    		dataset.addValue(y, "valor estatistica", x);
    		resultadofuncao = a + b*indiceRegressao;
    		dataset.addValue(resultadofuncao, "previsao", x);
    		indiceRegressao++;
    		
    	}
    	for(int h = 0; h < 10; h++){
    		resultadofuncao = a + b*indiceRegressao;
        	dataset.addValue(resultadofuncao, "previsao", "P"+Integer.toString(h));
        	indiceRegressao++;
    	}
    	
    	//faz segunda reta?
    	//faz terceira reta
    	scan.close();
    	
    	//anoIni = Integer.toString(anoIniBalde);
    	/*for(int ano = anoIniBalde; ano < anoIniBalde+20; ano++){
    		resultadofuncao = a + b*ano;
    		dataset.addValue(resultadofuncao, "previsao", Integer.toString(ano));
    	}*/
        return dataset;
        
    }
   
   /* private JFreeChart createChart(final XYDataset dataset) {
        
        final JFreeChart chart = ChartFactory.createXYLineChart(
            "Grafico",      // chart title
            "Periodo de Tempo",                      // x axis label
            "Valor da"+estatistica,                      // y axis label
            dataset,                  // data
            PlotOrientation.VERTICAL,
            true,                     // include legend
            true,                     // tooltips
            false                     // urls
        );

        // NOW DO SOME OPTIONAL CUSTOMISATION OF THE CHART...
        chart.setBackgroundPaint(Color.white);

//        final StandardLegend legend = (StandardLegend) chart.getLegend();
  //      legend.setDisplaySeriesShapes(true);
        
        // get a reference to the plot for further customisation...
        final XYPlot plot = chart.getXYPlot();
        plot.setBackgroundPaint(Color.lightGray);
    //    plot.setAxisOffset(new Spacer(Spacer.ABSOLUTE, 5.0, 5.0, 5.0, 5.0));
        plot.setDomainGridlinePaint(Color.white);
        plot.setRangeGridlinePaint(Color.white);
        
        final XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
        plot.setRenderer(renderer);

        // change the auto tick unit selection to integer units only...
        final NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
        rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
        // OPTIONAL CUSTOMISATION COMPLETED.
                
        return chart;
        
    }*/

    // ****************************************************************************
    // * JFREECHART DEVELOPER GUIDE                                               *
    // * The JFreeChart Developer Guide, written by David Gilbert, is available   *
    // * to purchase from Object Refinery Limited:                                *
    // *                                                                          *
    // * http://www.object-refinery.com/jfreechart/guide.html                     *
    // *                                                                          *
    // * Sales are used to provide funding for the JFreeChart project - please    * 
    // * support us so that we can continue developing free software.             *
    // ****************************************************************************
    
    /**
     * Starting point for the demonstration application.
     *
     * @param args  ignored.
     */
    public static void criaGrafico(String argUm, String argDois, String argOito) {

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
