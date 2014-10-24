//package replicamain;



import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Bhoomi
 */
public class ReplicaMain {
    
    public static HashMap<String, Integer> hostnamehashmap;
    public static  HashMap<Integer, Integer> porthashmap;
    public static  HashMap<Integer,Integer> voteshashmap; 
    public static  HashMap<Integer, String> indexToNameHashmap;
    
    public static volatile Hashtable<String,Integer> fileNameToVersionMap =new Hashtable<String,Integer>();
    
    
    public static  ArrayList<Votes> votesList=new ArrayList<Votes>();
    public static  int total_votes,r,w; 
    
    public static  ArrayList<String> listOfHosts=new ArrayList<String>();
    public static  int noOfOperations,generatorId,numberOfNodes;
    
    public static ArrayList<String> listOfFiles=new ArrayList<String>();
     public static ArrayList<String> listOfTestFiles=new ArrayList<String>();
    public static Hashtable<String, Lock> fileLockMap=new Hashtable<String, Lock>();
    
    static int myId;
    static String hostName;
    static File file;
    static int operationDelay;
    static int lines=0;
    static boolean testFlag=true;
    static int failNode=999;
    public static boolean failFlag=true;
    public static int opCount=0;
    
    public static void main(String args[]) throws FileNotFoundException, UnknownHostException, InterruptedException
    { //constructor
        hostnamehashmap = new HashMap<String, Integer>();
        porthashmap = new HashMap<Integer, Integer>();
       
        voteshashmap = new HashMap<Integer, Integer>();
        indexToNameHashmap = new HashMap<Integer,String>();

        
        int id;
        Scanner scanner = new Scanner(new File("BasicConfig.txt"));
        String fileName=null;
        
        
        
        for(int i=0;i<9;i++)
        {
            String line=scanner.nextLine();
            StringTokenizer token1 = new StringTokenizer(line,"=");
            token1.nextToken();
            if(line.startsWith("n"))
                numberOfNodes=Integer.parseInt(token1.nextToken());
            else
            if(line.startsWith("Votes"))
            {
                total_votes=Integer.parseInt(token1.nextToken());
               // System.out.println("Total votes :" + total_votes);
            }
            else if(line.startsWith("r"))
            {
                r=Integer.parseInt(token1.nextToken());
                //System.out.println("Read Quorum Size :" + r);
            }
                else if(line.startsWith("w"))
                {
                	w=Integer.parseInt(token1.nextToken());
                //	System.out.println("Write Quorum Size  :" + w);
                }
                	else if(line.startsWith("o"))
                	{
                noOfOperations=Integer.parseInt(token1.nextToken());
               // System.out.println("No of operations" + noOfOperations);
                
                	}
            else if(line.startsWith("g"))
            {
                generatorId=Integer.parseInt(token1.nextToken());
                //System.out.println("Generator ID :" + generatorId);
            }
            else if(line.startsWith("d"))
            {
                operationDelay=Integer.parseInt(token1.nextToken());
                //System.out.println("Operation Delay :" + operationDelay);
            }
            else if(line.startsWith("f"))
            {
            	fileName=token1.nextToken();
            	StringTokenizer fileToken=new StringTokenizer(fileName,",");
            	while(fileToken.hasMoreTokens())
            	{
            		String nameOfFile=fileToken.nextToken();
            		 ReplicaMain.modifyFileNameVersions("put",nameOfFile,1);	
                     Lock lock=new Lock(false,"shared",0,nameOfFile);
            		
                    listOfFiles.add(nameOfFile);
                    modifyFileLockMap("Put", nameOfFile,lock);           	
            	}
               
            	//System.out.println("FileName :" + fileName);
            }
            else if(line.startsWith("x"))
            {
            
            failNode=Integer.parseInt(token1.nextToken());
            }
        }
       
        
          
         while (scanner.hasNextLine())
        {
            String line = scanner.nextLine();
            StringTokenizer token = new StringTokenizer(line, " ");
           
            while (token.hasMoreTokens())
            {
               
                    id = Integer.parseInt(token.nextToken());
                 
              
                    
                    //System.out.println("ID : " + id + "  ");
                    String nameOfHost=token.nextToken();
                    //System.out.print("HostName :" + nameOfHost + "  ");
                    
                    
                    hostnamehashmap.put(nameOfHost,id);
                    indexToNameHashmap.put(id, nameOfHost);
                    listOfHosts.add(nameOfHost);
                    int portNumber=Integer.parseInt(token.nextToken());
                    //System.out.print("Port Number :" + portNumber + " ");
                    porthashmap.put(id, portNumber);
                    
                    int numberOfVotes=Integer.parseInt(token.nextToken());
                    //System.out.print("Number of votes" + numberOfVotes + " ");
                    voteshashmap.put(id, numberOfVotes);
                    
                    //System.out.println();
                    //System.out.println("Adding vote :" + numberOfVotes + "with ID : " + id  +  " to sorted votes list");
                    makeSortedVotesList(id, numberOfVotes);
                    //System.out.println();
                 }
          
                    
          
        }
       
      
       hostName=java.net.InetAddress.getLocalHost().getHostName();  
       myId=hostnamehashmap.get(hostName);
       
      
       for(String s : listOfFiles)
       {
          
           try
           {
               File replicaFile = new File(s+myId+".txt");
               if(new File(s+myId+".txt").exists())
               {
                   new File(s+myId+".txt").delete();
               }
               replicaFile.createNewFile();
               System.out.println(s+myId+".txt created");
               BufferedWriter out1 = new BufferedWriter(new FileWriter(s+myId+".txt",true));
               out1.write("READING FROM "+s + myId);
               out1.flush();
               //System.out.println("READING FROM FILEABC" +id);
               out1.newLine();
               
                        File testFile = new File("Test"+s+myId+".txt");
                        if(testFile.exists())
                            {
                            testFile.delete();
                            }
                        testFile.createNewFile();
                        listOfTestFiles.add("Test"+s+myId+".txt");
                        
               
               
               
               
               
           } catch (IOException ex) {
               Logger.getLogger(ReplicaMain.class.getName()).log(Level.SEVERE, null, ex);
           }
       }
    
       if(myId==generatorId)
    {
      // System.out.println("I am the generator");
       GenerateOperations generateOperations=new GenerateOperations();
            try {
                generateOperations.generateFileOperations(noOfOperations, numberOfNodes);
            } catch (IOException ex) {
                Logger.getLogger(ReplicaMain.class.getName()).log(Level.SEVERE, null, ex);
            }
       //System.out.println("Done with generation of operations");
       Thread.sleep(40000);
    
    } else
		try {
			Thread.sleep(30000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    FileHandler fh;  
    
       int portNumber = porthashmap.get(myId);
       int ownVotes = voteshashmap.get(myId);
       
       
       
       Server myserver = new Server(myId,portNumber);
       System.out.println("Server Ready");
       Thread.sleep(10000);
      // Thread.sleep(myId*1000);
       
       Scanner scanFile = new Scanner(new File("OpFile.txt"));
       
       //Scanner scanFile = new Scanner(new File("op1.txt"));
       
       
     
       
       System.out.println("Ready to perform");
       
       
       /*if(myId==failNode)
       {
          opCount=0;
           
           //long c=System.currentTimeMillis();
           //HandleFailure hf=new HandleFailure(c, 6000);
       }  */  
       while (scanFile.hasNextLine())
       {
    	   String currentLine = scanFile.nextLine();
    	
           StringTokenizer token = new StringTokenizer(currentLine, " ");
          
           while (token.hasMoreTokens())
           {
               int nodeId = Integer.parseInt(token.nextToken());
               String operationName=token.nextToken();
               String nameOfFile=token.nextToken();
    	   
              
               
           if(myId==nodeId && failFlag==true)
           {
        	   
        	  // Random randomGenerator=new Random();
               //int delay=randomGenerator.nextInt(operationDelay);
              
               System.out.println("I have to perform following operation");
               System.out.println("Node ID : " + nodeId + " Operation Name :" + operationName + " name of file : " + nameOfFile);  
         //      System.out.println("Calling sender code");
                     
               Sender s=new Sender(nodeId,operationName,nameOfFile,ownVotes);
               opCount++; 
               
               Thread.sleep(operationDelay);
            }
           
           
           }
     
           
           
           
           
       }
       
       
       if(myId==0)
       {     
       
       //test code
           Thread.sleep(numberOfNodes*noOfOperations*25000);
            
       
           
               
    System.out.println("Testing : ");
    
    
    for(String d : listOfTestFiles)
    {
    
               try {
                   lines = 0;
                   
                   BufferedReader br = new BufferedReader(new FileReader(d));
                   br.readLine();
                   while (br.readLine() != null)
                   {
                       
                       lines++;
                   }
                   
                   
                   
                   if(lines!=0)
                   {
                       
                       
                       
                       long[] Array1 = new long[lines];
                       long[] Array2 = new long[lines];
                       String[] OperationName = new String[lines];
                       
                       Scanner scannerTest = new Scanner(new File(d));
                       int i = 0;
                       scannerTest.nextLine();
                       while (scannerTest.hasNextLine())
                       {
                           String currentLineTest = scannerTest.nextLine();
                           
                           
                           StringTokenizer token1 = new StringTokenizer(currentLineTest, " ");
                           System.out.println(currentLineTest);
                           while (token1.hasMoreTokens())
                           {
                               Array1[i] = Long.parseLong(token1.nextToken());
                               Array2[i]= Long.parseLong(token1.nextToken());
                               int node = Integer.parseInt(token1.nextToken());
                               OperationName[i] = token1.nextToken();
                               
                               
                           }
                           i++;
                       }
                       
                       for(int j = 0; j < (Array2.length)-1; j++){
                           if(Array2[j] > Array1[j+1] ){
                               testFlag=false;
                               
                               break;
                           }
                       }
                       
                       
                       
                   }   } catch (IOException ex) {
                   Logger.getLogger(ReplicaMain.class.getName()).log(Level.SEVERE, null, ex);
               }
            }
    
            if(testFlag==true)
                           System.out.println("Program worked Correctly");
                       else
                           System.out.println("Program did not work Correctly");
            }
    
           }
       
       
    

    
    public static synchronized Integer modifyFileNameVersions(String opName,String nameOfFile, Integer versionNumber) 
    {
        if(opName.equalsIgnoreCase("put"))
        {
    	if(!ReplicaMain.fileNameToVersionMap.containsKey(nameOfFile))
    	{
    	   ReplicaMain.fileNameToVersionMap.put(nameOfFile, 1);
    	}
    	else
    	{
    		int versionId=ReplicaMain.fileNameToVersionMap.get(nameOfFile);
    		versionId=versionNumber;
    		ReplicaMain.fileNameToVersionMap.put(nameOfFile, versionId);
    	}
        return null;
        }
        
        else if(opName.equalsIgnoreCase("get"))
        {
            return ReplicaMain.fileNameToVersionMap.get(nameOfFile);
        }
        else
            return null;
        
    }
    
    public  static void makeSortedVotesList(int id,int vote)
    {
    	if(votesList.isEmpty())
    	{
    		Votes v=new Votes(id, vote);
    		votesList.add(0, v);
    		//System.out.println("Vote : " + vote + " added to sorted list with id : " + id );
    	 
    	}
    	else
    	{
    
    	boolean addedFlag=false;	
        for(int i=0;i<votesList.size();i++)
        {
        	
        	if(vote < (votesList.get(i).getVotes()))
        	{
        		continue;
        	}
        	else
        	{
        		Votes v=new Votes(id, vote);
        		votesList.add(i, v);
        		//System.out.println("Vote : " + vote + " added to list with id : " + id );
        	    addedFlag=true;
        		break;
        	}
        }
         
        if(!addedFlag)
        {
        	Votes v=new Votes(id, vote);
    		votesList.add(votesList.size(), v);
    		//System.out.println("Vote : " + vote + " added to list with id : " + id );
        }
        
    	}
    }
  



    public static synchronized Lock modifyFileLockMap(String operation,String key, Lock lock)
    {
    	if(operation.equalsIgnoreCase("Put"))
    	{
    		fileLockMap.put(key,lock);
    		return null;
    	}
    
    	else
    	if(operation.equalsIgnoreCase("Get"))	
    	{
    	  Lock lockValue=fileLockMap.get(key);
    	  return lockValue;
    	}
    	else
		return null;
    
    }
}
    
