package mapreduce;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AggregateJob extends Configured implements Tool {
	public int firstRun(String[] args) throws Exception {

		Configuration conf = getConf();
		conf.set("agregador", args[3]);
		conf.set("dado", args[4]);

		//JobConf job = new JobConf(conf);

		Job job = Job.getInstance(conf);

		job.setJarByClass(getClass());
		job.setJobName(getClass().getSimpleName());

		if(!args[7].equals("ZZ"))
			FileInputFormat.setInputPathFilter(job, PaisFilter.class);
		//FileInputFormat.addInputPath(job, new Path(args[0]+"/"+args[5])); //new Path(args[0]+"/*nome_da_pasta" para que arquivos dentro de pastas tambem sejam lidos
		int anoIni = 0;
		int anoFim = Integer.parseInt(args[6]);
		for(anoIni = Integer.parseInt(args[5]); anoIni <= anoFim; anoIni++){
			MultipleInputs.addInputPath(job, new Path(args[0]+anoIni), CombinedInputFormat.class, StatisticsMapper.class);
		}
		FileOutputFormat.setOutputPath(job, new Path(args[1]+args[2]));

		job.setMapperClass(StatisticsMapper.class);
		//job.setCombinerClass(MediaReducer.class);
		job.setReducerClass(StatisticsReducer.class);


		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(CompositeWritable.class);

		//job.setInputFormatClass(SingleInputFormat.class);

		job.setNumReduceTasks(1);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public int secondRun(String[] args) throws Exception {

		Configuration conf = getConf();
		conf.set("agregador", args[3]);
		conf.set("estatistica", args[8]);

		Job job = Job.getInstance(conf);

		job.setJarByClass(getClass());
		job.setJobName(getClass().getSimpleName());

		FileInputFormat.addInputPath(job, new Path(args[1]+"/"+args[2]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"/"+args[2]+"/regressao"+args[8]));

		job.setMapperClass(RegressionMapper.class);
		//job.setCombinerClass(MediaReducer.class);
		job.setReducerClass(RegressionReducer.class);


		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(CompositeWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(CompositeWritable.class);

		//job.setInputFormatClass(SingleInputFormat.class);

		job.setNumReduceTasks(1);

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
		String pais = null;
		String estatistica = null;
		String[] params = new String[9];

		File f = null;;
		boolean tratamento = false;
		while(tratamento == false)
		{
			System.out.println("Digite o caminho para a pasta onde se encontram os dados: ");
			entrada = scan.next();
			if(entrada.charAt(entrada.length()-1) != '/')
				entrada += "/";
			f = new File(entrada);
			if(f.exists() && f.isDirectory())
				tratamento = true;
			else
				System.out.println("Erro ao encontrar a pasta, tente novamente");
			params[0] = entrada;
		}

		tratamento = false;
		while(tratamento == false)
		{
			System.out.println("\nDigite o caminho para a raiz da pasta onde deseja armazenar os dados: ");
			raizSaida = scan.next();
			if(raizSaida.charAt(raizSaida.length()-1) != '/')
				raizSaida += "/";
			f = new File(raizSaida);
			if(f.exists() && f.isDirectory())
				tratamento = true;
			else
				System.out.println("Erro ao encontrar a pasta, tente novamente");
			params[1] = raizSaida;
		}

		tratamento = false;
		while(tratamento == false){
			System.out.println("Digite qual dado deseja analisar. "
					+ "\nOpcoes: "
					+ "\n\tMedTemp = Temperatura media"
					+ "\n\tMedCond = Ponto medio de condensassao da agua"
					+ "\n\tMedMar = Media da pressao a nivel do mar"
					+ "\n\tMedPressao = Pressao media"
					+ "\n\tMedVento = Velocidade do vento media"
					+ "\n\tMaxVento = Velocidade maxima do vento"
					+ "\n\tMaxRajada = Velocidade maxima das rajadas de vento"
					+ "\n\tMaxTemp = Maximas da temperatura"
					+ "\n\tMinTemp = Minimas da temperatura"
					+ "\n\tPrecip = Quantidade de precipitacao"
					+ "\n\tNeve = Profundidade da neve"
					+ "\n");
			dado = scan.next().toUpperCase();
			String[] opcoes = {"MEDTEMP", "MEDCOND", "MEDMAR", "MEDPRESSAO", "MEDVENTO", "MAXVENTO", "MAXRAJADA", "MAXTEMP", "MINTEMP", "PRECIP", "NEVE"};
			for(int c = 0; c < opcoes.length; c++)
			{
				if(dado.equals(opcoes[c]))
				{
					tratamento = true;
					c = opcoes.length;
				}					
			}
			if(tratamento == false)
				System.out.println("Opcao invalida, tente novamente");
		}

		tratamento = false;
		while(tratamento == false)
		{
			System.out.println("\nDigite sobre qual intervalo de anos deseja realizar a operacao: ");
			System.out.println("Inicio:");
			anoIni = scan.next();
			if(!anoIni.matches("[0-9]+") || anoIni.length() != 4 || Integer.parseInt(anoIni) < 1901 || Integer.parseInt(anoIni) > 2016)
			{
				System.out.println("Ano inicial invalido, tente novamente");
				continue;
			}
			System.out.println("\nFim:");
			anoFim = scan.next();
			if(!anoFim.matches("[0-9]+") || anoFim.length() != 4 || Integer.parseInt(anoFim) < 1901 || Integer.parseInt(anoFim) > 2016 || Integer.parseInt(anoIni) > Integer.parseInt(anoFim))
			{
				System.out.println("Ano final invalido, tente novamente");
				continue;
			}
			tratamento = true;
		}	

		tratamento = false;
		while(tratamento == false)
		{
			System.out.println("\nDigite como deseja agregar os dados"
					+ "\nOpcoes: "
					+ "\n\tAno"
					+ "\n\tMes"
					+ "\n\tSemana");
			agregador = scan.next().toUpperCase();
			if(agregador.equals("ANO") || agregador.equals("MES") || agregador.equals("SEMANA"))
				tratamento = true;
			else
				System.out.println("Opcao invalida, tente novamente");
		}
		tratamento = false;
		while(tratamento == false){
			System.out.println("\nPaises Disponiveis: "
					+ "\nAA          ARUBA"                                                                           
					+ "\nAC          ANTIGUA AND BARBUDA"                                                             
					+ "\nAF          AFGHANISTAN"                                                                     
					+ "\nAG          ALGERIA"                                             
					+ "\nAJ          AZERBAIJAN"                                                                      
					+ "\nAL          ALBANIA"                                                                         
					+ "\nAM          ARMENIA"                                                      
					+ "\nAO          ANGOLA"                                                                          
					+ "\nAQ          AMERICAN SAMOA"                                                                  
					+ "\nAR          ARGENTINA"                                                                       
					+ "\nAS          AUSTRALIA"                                  
					+ "\nAU          AUSTRIA"                                                                         
					+ "\nAV          ANGUILLA"                         
					+ "\nAY          ANTARCTICA"                                                                      
					+ "\nAZ          AZORES"                                                                          
					+ "\nBA          BAHRAIN"
					+ "\nBB          BARBADOS"                                                                        
					+ "\nBC          BOTSWANA"                                                                       
					+ "\nBD          BERMUDA"                                                                         
					+ "\nBE          BELGIUM"                                                                         
					+ "\nBF          BAHAMAS THE"                                                                     
					+ "\nBG          BANGLADESH"                                                                      
					+ "\nBH          BELIZE"                                                                          
					+ "\nBK          BOSNIA AND HERZEGOVINA"                                                          
					+ "\nBL          BOLIVIA"                                                                         
					+ "\nBM          BURMA"                                                                           
					+ "\nBN          BENIN"                                                                           
					+ "\nBO          BELARUS"                                                                         
					+ "\nBP          SOLOMON ISLANDS"                                            
					+ "\nBR          BRAZIL"                                              
					+ "\nBT          BHUTAN"                                                                          
					+ "\nBU          BULGARIA"                                                                        
					+ "\nBV          BOUVET ISLAND"                                                                   
					+ "\nBX          BRUNEI"                                                                         
					+ "\nBY          BURUNDI"                                       
					+ "\nCA          CANADA"                                                                          
					+ "\nCB          CAMBODIA"                                                                                                                                       
					+ "\nCD          CHAD"                                                                            
					+ "\nCE          SRI LANKA"                                                                       
					+ "\nCF          CONGO"                                                                           
					+ "\nCG          ZAIRE"                                                                           
					+ "\nCH          CHINA"                                                                           
					+ "\nCI          CHILE"                                                                           
					+ "\nCJ          CAYMAN ISLANDS"                                                                  
					+ "\nCK          COCOS (KEELING) ISLANDS"                                              
					+ "\nCM          CAMEROON"                                                                        
					+ "\nCN          COMOROS"                                                                         
					+ "\nCO          COLOMBIA"                                                                        
					+ "\nCP          CANARY ISLANDS"                                                                  
					+ "\nCQ          NORTHERN MARIANA ISLANDS"                                             
					+ "\nCS          COSTA RICA"                                                                      
					+ "\nCT          CENTRAL AFRICAN REPUBLIC"                                                        
					+ "\nCU          CUBA"                                                                            
					+ "\nCV          CAPE VERDE"                                                                      
					+ "\nCW          COOK ISLANDS"                                                                    
					+ "\nCY          CYPRUS"                                                
					+ "\nDA          DENMARK"                                                                         
					+ "\nDJ          DJIBOUTI"                                                                        
					+ "\nDO          DOMINICA"                                               
					+ "\nDR          DOMINICAN REPUBLIC"                                                                                                                              
					+ "\nEC          ECUADOR"                                                                         
					+ "\nEG          EGYPT"                                                                           
					+ "\nEI          IRELAND"                                                                         
					+ "\nEK          EQUATORIAL GUINEA"                                                               
					+ "\nEN          ESTONIA"                                                                         
					+ "\nER          ERITREA"                                                                         
					+ "\nES          EL SALVADOR"                                                                     
					+ "\nET          ETHIOPIA"                                                                        
					+ "\nEU          EUROPA ISLAND"                                                                   
					+ "\nEZ          CZECH REPUBLIC"                                                                  
					+ "\nFG          FRENCH GUIANA"                                                                   
					+ "\nFI          FINLAND"                                                                         
					+ "\nFJ          FIJI"                                                                            
					+ "\nFK          FALKLAND ISLANDS (ISLAS MALVINAS)"                                               
					+ "\nFM          MICRONESIA, FEDERATED STATES OF"                                                 
					+ "\nFO          FAROE ISLANDS"                                                                   
					+ "\nFP          FRENCH POLYNESIA"                                                                
					+ "\nFQ          BAKER ISLAND"                                                                    
					+ "\nFR          FRANCE"                                                                          
					+ "\nFS          FRENCH SOUTHERN AND ANTARCTIC LANDS"                                             
					+ "\nGA          GAMBIA  THE"                                                                     
					+ "\nGB          GABON"                                                                           
					+ "\nGG          GEORGIA"                                                                         
					+ "\nGH          GHANA"                                                                           
					+ "\nGI          GIBRALTAR"                                                                       
					+ "\nGJ          GRENADA"                                                                         
					+ "\nGK          GUERNSEY"                                                                        
					+ "\nGL          GREENLAND"                                                                       
					+ "\nGM          GERMANY"                                              
					+ "\nGP          GUADELOUPE"                                                                      
					+ "\nGQ          GUAM"                                                                            
					+ "\nGR          GREECE"                                                                          
					+ "\nGT          GUATEMALA"                                                                       
					+ "\nGV          GUINEA"                                                                          
					+ "\nGY          GUYANA"                                                      
					+ "\nHA          HAITI"                         
					+ "\nHK          HONG KONG"                              
					+ "\nHO          HONDURAS"                                                 
					+ "\nHR          CROATIA"                                                                         
					+ "\nHU          HUNGARY"                                                                         
					+ "\nIC          ICELAND"                                                                         
					+ "\nID          INDONESIA"                                                                       
					+ "\nIM          MAN  ISLE OF"                                                                    
					+ "\nIN          INDIA"                                                                           
					+ "\nIO          BRITISH INDIAN OCEAN TERRITORY"                                            
					+ "\nIR          IRAN"                                                                            
					+ "\nIS          ISRAEL"                                                                          
					+ "\nIT          ITALY"                                                                           
					+ "\nIV          COTE D'IVOIRE"                                              
					+ "\nIZ          IRAQ"                                                                            
					+ "\nJA          JAPAN"                                                                           
					+ "\nJE          JERSEY"                                                                          
					+ "\nJM          JAMAICA"                                                                                                                                               
					+ "\nJO          JORDAN"                                                                          
					+ "\nJQ          JOHNSTON ATOLL"                                                                  
					+ "\nJU          JUAN DE NOVA ISLAND"                                                             
					+ "\nKE          KENYA"                                                                           
					+ "\nKG          KYRGYZSTAN"                                                                      
					+ "\nKN          KOREA, NORTH"                                           
					+ "\nKR          KIRIBATI"                                                                        
					+ "\nKS          KOREA, SOUTH"                                                                    
					+ "\nKT          CHRISTMAS ISLAND"                                                                
					+ "\nKU          KUWAIT"                                                                          
					+ "\nKZ          KAZAKHSTAN"                                                                      
					+ "\nLA          LAOS"                                   
					+ "\nLE          LEBANON"                                                                         
					+ "\nLG          LATVIA"                                                                          
					+ "\nLH          LITHUANIA"                                                                       
					+ "\nLI          LIBERIA"                                     
					+ "\nLO          SLOVAKIA"                                                                        
					+ "\nLQ          PALMYRA ATOLL"                                                                   
					+ "\nLS          LIECHTENSTEIN"                                                                   
					+ "\nLT          LESOTHO"                                                                         
					+ "\nLU          LUXEMBOURG"                                                                      
					+ "\nLY          LIBYA"                                                                           
					+ "\nMA          MADAGASCAR"                                                                      
					+ "\nMB          MARTINIQUE"                                                                      
					+ "\nMC          MACAU"                                                                           
					+ "\nMD          MOLDOVA"                                                          
					+ "\nMF          MAYOTTE"                                                                         
					+ "\nMG          MONGOLIA"                                                                        
					+ "\nMH          MONTSERRAT"                                                                      
					+ "\nMI          MALAWI"                                                                          
					+ "\nMK          MACEDONIA"                                                                       
					+ "\nML          MALI"                                                       
					+ "\nMO          MOROCCO"                                                                         
					+ "\nMP          MAURITIUS"                                                                       
					+ "\nMQ          MIDWAY ISLANDS"                                                                  
					+ "\nMR          MAURITANIA"                                                                      
					+ "\nMT          MALTA"                                                                           
					+ "\nMU          OMAN"                                                                            
					+ "\nMV          MALDIVES"                                            
					+ "\nMX          MEXICO"                                                                          
					+ "\nMY          MALAYSIA"                                                                        
					+ "\nMZ          MOZAMBIQUE"                                                                      
					+ "\nNC          NEW CALEDONIA"                                                                   
					+ "\nNE          NIUE"                                                                            
					+ "\nNF          NORFOLK ISLAND"                                                                  
					+ "\nNG          NIGER"                                                                           
					+ "\nNH          VANUATU"                                                                         
					+ "\nNI          NIGERIA"                                                                         
					+ "\nNL          NETHERLANDS"                                                                     
					+ "\nNO          NORWAY"                                                                          
					+ "\nNP          NEPAL"                                                                           
					+ "\nNR          NAURU"                                                                           
					+ "\nNS          SURINAME"                                                                        
					+ "\nNT          NETHERLANDS ANTILLES"                                                            
					+ "\nNU          NICARAGUA"                                                                       
					+ "\nNZ          NEW ZEALAND"                                                
					+ "\nPA          PARAGUAY"                                                                        
					+ "\nPC          PITCAIRN ISLANDS"                                                                
					+ "\nPE          PERU"                                         
					+ "\nPK          PAKISTAN"                                                                        
					+ "\nPL          POLAND"                                                                          
					+ "\nPM          PANAMA"                                                                          
					+ "\nPN          NORTH PACIFIC ISLANDS"                                                           
					+ "\nPO          PORTUGAL"                                                                        
					+ "\nPP          PAPUA NEW GUINEA"                                                                
					+ "\nPS          PALAU - TRUST TERRITORY OF THE PACIFIC ISLANDS"                                  
					+ "\nPU          GUINEA-BISSAU"                                                         
					+ "\nQA          QATAR"                                                                           
					+ "\nRE          REUNION AND ASSOCIATED ISLANDS"                                                  
					+ "\nRM          MARSHALL ISLANDS"                                                                
					+ "\nRO          ROMANIA"                                                                         
					+ "\nRP          PHILIPPINES"                                                                     
					+ "\nRQ          PUERTO RICO"                                                                     
					+ "\nRS          RUSSIA"                                                                          
					+ "\nRW          RWANDA"                                                                          
					+ "\nSA          SAUDI ARABIA"                                                                    
					+ "\nSB          ST. PIERRE AND MIQUELON"                                                         
					+ "\nSC          ST. KITTS AND NEVIS"                                                             
					+ "\nSE          SEYCHELLES"                                                                      
					+ "\nSF          SOUTH AFRICA"                                                                    
					+ "\nSG          SENEGAL"                                                                         
					+ "\nSH          ST. HELENA"                                                                      
					+ "\nSI          SLOVENIA"                              
					+ "\nSL          SIERRA LEONE"                                            
					+ "\nSN          SINGAPORE"                                                                       
					+ "\nSO          SOMALIA"                                                                         
					+ "\nSP          SPAIN"                                                          
					+ "\nST          ST. LUCIA"                                                                       
					+ "\nSU          SUDAN"                                                                           
					+ "\nSV          SVALBARD"                                                                        
					+ "\nSW          SWEDEN"                                                                          
					+ "\nSX          SOUTH GEORGIA AND THE SOUTH SANDWICH ISLANDS"                                    
					+ "\nSY          SYRIA"                                                                           
					+ "\nSZ          SWITZERLAND"						                                                            
					+ "\nTD          TRINIDAD AND TOBAGO"                                                                                                                             
					+ "\nTH          THAILAND"                                                                        
					+ "\nTI          TAJIKISTAN"                                                                      
					+ "\nTK          TURKS AND CAICOS ISLANDS"                                                        
					+ "\nTL          TOKELAU"                                                                         
					+ "\nTN          TONGA"                                                                           
					+ "\nTO          TOGO"                                                                            
					+ "\nTP          SAO TOME AND PRINCIPE"                                                           
					+ "\nTS          TUNISIA"                                                                         
					+ "\nTU          TURKEY"                                                                          
					+ "\nTV          TUVALU"                                                                          
					+ "\nTW          TAIWAN"                                                                          
					+ "\nTX          TURKMENISTAN"                                                                    
					+ "\nTZ          TANZANIA"                                                                                                                         
					+ "\nUG          UGANDA"                                                                          
					+ "\nUK          UNITED KINGDOM"                                                                  
					+ "\nUP          UKRAINE"                                                                         
					+ "\nUS          UNITED STATES"                                                                   
					+ "\nUV          BURKINA FASO"                                                                    
					+ "\nUY          URUGUAY"                                                                         
					+ "\nUZ          UZBEKISTAN"                                                                      
					+ "\nVC          ST. VINCENT AND THE GRENADINES"                                                  
					+ "\nVE          VENEZUELA"                                                                       
					+ "\nVI          VIRGIN ISLANDS (BRITISH)"                                                        
					+ "\nVM          VIETNAM"                                                                         
					+ "\nVQ          VIRGIN ISLANDS (U.S.)"                                                                   
					+ "\nWA          NAMIBIA"                                                                                                                                              
					+ "\nWF          WALLIS AND FUTUNA"                                                               
					+ "\nWI          WESTERN SAHARA"                                                                  
					+ "\nWQ          WAKE ISLAND"                                                                     
					+ "\nWS          WESTERN SAMOA"                                                                   
					+ "\nWZ          SWAZILAND"                                                                       
					+ "\nYM          YEMEN"                                     
					+ "\nZA          ZAMBIA"                                                                        
					+ "\nZI          ZIMBABWE"
					+ "\nZZ		     TODOS OS PAISES"
					+ "\nDigite a sigla do pais que deseja realizar a analise: (Digite ZZ para usar todos os paises");
			pais = scan.next().toUpperCase();
			if (!pais.equals("ZZ"))
			{
				/*String pastaPaises = "";
				boolean tratamentoInterno = false;
				while(tratamentoInterno == false)
				{
					System.out.println("\n Digite o caminho para a pasta raiz onde encontra-se a pasta Paises: \n");
					pastaPaises = scan.next();
					f = new File(pastaPaises+"/Paises");
					if(f.exists())
						tratamentoInterno = true;
					else
						System.out.println("Pasta invalida, tente novamente");
				}	*/				
				EstacoesPais lista = EstacoesPais.getInstance();
				try
				{
					lista.criaListaEstacoes(/*pastaPaises,*/ pais);
					tratamento = true;
				}
				catch(FileNotFoundException e)
				{
					System.out.println("Opcao de pais invalida, tente novamente");
				}
			}
			tratamento = true;
		}				
		tratamento = false;
		while(tratamento == false)
		{
			System.out.println("\nDigite o nome da pasta do arquivo de saida (nao repita nomes): \n");
			pastaSaida = scan.next();
			f = new File(raizSaida+pastaSaida);
			if(!f.exists())
				tratamento = true;
			else
				System.out.println("Pasta ja existente, tente novamente");
		}
		tratamento = false;
		while(tratamento == false){
			System.out.println("Sobre qual estatistica deseja realizar uma regressao?"
					+ "\n Digite N caso nao deseje realizar nenhuma regressao."
					+ "\n\tMedia"
					+ "\n\tDesvioPadrao"
					+ "\n\tVariancia");
			estatistica = scan.next().toUpperCase();
			if(estatistica.equals("MEDIA") || estatistica.equals("DESVIOPADRAO") || estatistica.equals("VARIANCIA") || estatistica.equals("N"))
				tratamento = true;
			else
				System.out.println("Opcao invalida, tente novamente!");
		}

		params[2] = pastaSaida;
		params[3] = agregador;
		params[4] = dado;
		params[5] = anoIni;
		params[6] = anoFim;
		params[7] = pais;
		params[8] = estatistica;

		while(true)//mantendo o while true de antes
		{

			// Roda run() com a lista de argumentos passados na execucao do jar
			// + argumentos obtidos interativamente atraves das perguntas acima.
			int rc = ToolRunner.run(new AggregateJob(), params);
			System.exit(rc);
		}

	}

	public int run(String[] args) throws Exception {//so para parar de dar erro
		firstRun(args);
		if(!args[8].equals("N")){
			secondRun(args);	
		}
		try{
			Grafico.criaGrafico(args[1], args[2], args[8]);
			Scanner scan = new Scanner(System.in);
			scan.next();
			scan.close();
			System.out.println("Execucao concluida com sucesso! \nFeche o grafico para encerrar o programa!");
		}
		catch(Exception e)
		{
			System.out.println("Nao ha dados na selecao escolhida o suficiente para gerar a regressao, tente novamente com outras opcoes!");
			System.exit(0);
		}
		return 0;
	}
}
