import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;


public class GenerateOperations {

	public void generateFileOperations(int noOfOperations,int numberOfNodes) throws IOException
	{
	
            
            
            File operationFile = new File("OpFile.txt");
	       //File operationFile = new File("op1.txt");
	        
	            try
	            {
			 if(new File("OpFile.txt").exists())
                         {
                            new File("OpFile.txt").delete();      
                         }
		
                        operationFile.createNewFile();
                    } 
	            catch (IOException e)
	            {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	       
	
	BufferedWriter out = new BufferedWriter(new FileWriter("OpFile.txt",true));
	
	//System.out.println("This is in the generation method");
	System.out.println("Number of nodes : " + numberOfNodes);
     
        
        double readCo=noOfOperations*numberOfNodes*0.6;
        
        int readCounter=(int)Math.ceil(readCo);
        System.out.println("No of read operation : "+readCounter);
        
        int writeCounter=(noOfOperations*numberOfNodes)-readCounter;
        System.out.println("No of write operation : "+writeCounter);
        
        
        Random randomGenerator=new Random();
        for(int a=0;a<numberOfNodes;a++)
        {
           // System.out.println("For node : "+a);
            int nodeId=a;
            
            for(int k=0;k<noOfOperations;k++)
            {
             //   System.out.println("operation "+k);
                int fileId=randomGenerator.nextInt(ReplicaMain.listOfFiles.size());
                String fileName=ReplicaMain.listOfFiles.get(fileId);
                
                int fileOp=randomGenerator.nextInt(2);
               // System.out.println("fileOp : "+fileOp +" readCounter : "+readCounter+" writeCounter : "+writeCounter);
                String readWrite=null;
                if(fileOp==0 && readCounter==0)
                {//   System.out.println("read are over");
                    fileOp=1;}
                if(fileOp==1 && writeCounter==0)
                {
                   // System.out.println("write are over");
                    fileOp=0;}
                
                
                
                if(fileOp==0)
                {
                    readWrite="read";
                    //System.out.println("fileOp=0");
                    readCounter--;
                }    
                else
                {
                    readWrite="write";
                    //System.out.println("fileOp=1");
                    writeCounter--;
                }
                    
                    
                     out.write(nodeId + " " + readWrite + " " + fileName);
                     out.flush();
                     System.out.println(nodeId + " " + readWrite + " " + fileName);
                     out.newLine();              
                }
                
            }
        }
        
     

	public void performOperation(int nodeId, String nameOfFile,
			ArrayList<Integer> quoromMembers, String operationName, HashMap<Integer,
			Integer> quorumNodeToVersionHashMap, HashMap<Integer, Socket> quorumNodeToSocketMap,
        HashMap<Integer, ObjectOutputStream> quorumNodeToOutputStreamMap,boolean ownVotesSuffiecientFlag,Long startingTime) 
	{
		
            BufferedWriter testOut = null;
            try
            {
            testOut = new BufferedWriter(new FileWriter("Test"+nameOfFile+ReplicaMain.myId+".txt",true));
            Scanner testScanner=new Scanner(new File("Test"+nameOfFile+ReplicaMain.myId+".txt"));
            Lock lock=ReplicaMain.modifyFileLockMap("Get", nameOfFile, null);
           
            int ownVersionNumber=ReplicaMain.modifyFileNameVersions("get",nameOfFile, null);
            
            String writingFile= nameOfFile + nodeId + ".txt";        
            int latestVersion=ownVersionNumber;
            if(!ownVotesSuffiecientFlag)     
            {
                    
                    
                    
                    int latestNodeId=ReplicaMain.myId;
                   // System.out.println("Quorum members: ");
                    /*for(int k : quoromMembers)
                    {
                        System.out.print(k+" ");
                    }*/
                    //System.out.println("key set :");
                    for(Integer key : quorumNodeToVersionHashMap.keySet())
                    {
                      //  System.out.print(key + " ");
                        if(quorumNodeToVersionHashMap.get(key) >latestVersion)
                        {
                            
                            latestVersion=quorumNodeToVersionHashMap.get(key);
                            latestNodeId=key;
                        }
                        
                    }
                    System.out.println("latestVersion : "+latestVersion);
                    System.out.println("latestNodeId : "+latestNodeId);
                    String readingFile = nameOfFile + latestNodeId + ".txt";
                    
                    //  PrintWriter pw=null;
                        try {
                        Scanner  scanner = new Scanner(new File(readingFile));
                        System.out.println("Reading file: "+readingFile);
                        System.out.println("Writing file: "+writingFile);
                        
                        if(latestVersion!=ownVersionNumber)
                        {
                            
                            ReplicaMain.modifyFileNameVersions("put",nameOfFile, latestVersion);
                            System.out.println("Copying latest copy");
                            BufferedWriter copyWrite = new BufferedWriter(new FileWriter(writingFile));
                            
                            
                            System.out.println("Before while");
                            while(scanner.hasNextLine())
                            {
                               
                                    String line=scanner.nextLine();
                                    System.out.println("Line read from reading file : "+line);
                                    if(line!=null)
                                    {
                                        
                                        
                                        copyWrite.write(line);
                                        copyWrite.newLine();
                                        copyWrite.flush();
                                        // fw.println(line);
                                        
                                    
                                   
                                }
                                
                            }
                            
                            
                        }
                    }
                    catch (FileNotFoundException ex)
                    {
                        Logger.getLogger(GenerateOperations.class.getName()).log(Level.SEVERE, null, ex);
                    } catch (IOException ex) {
                        Logger.getLogger(GenerateOperations.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    
                }
                try
                {
                    if(operationName.equalsIgnoreCase("read"))
                    {
                        

                        @SuppressWarnings("resource")
                                Scanner scannerRead = new Scanner(new File(writingFile));
                        while(scannerRead.hasNextLine())
                        {
                            String line=scannerRead.nextLine();
                            
                            if(line!=null)
                            {
                                System.out.println(line);
                                
                            }
                        }
                        
                        if(ownVotesSuffiecientFlag)
                        {
                            lock.updateNumberOfGrants(false);
                            if(lock.getNumberOfGrants()==0)
                            {
                                lock.setLockValue(false,"shared");
                               
                
				 while(testScanner.hasNextLine())
				 {
					String line2=testScanner.nextLine();
                        
                                        if(line2!=null && line2.startsWith(startingTime.toString()))
                                        {
                                        Long time2 = System.nanoTime();

                                        testOut.write(time2.toString()+" "+ReplicaMain.myId+" "+operationName);
                                        System.out.println("UNLOCKING TIME IN Test"+nameOfFile+ReplicaMain.myId+".txt :"+ time2.toString());
                                        testOut.flush();
                                       
                            
                                        }
				 
				 
				 }
                            }
                        }
                        
                    }
                    
                    else if(operationName.equalsIgnoreCase("write"))
                    {
                        //System.out.println("Inside write block");
                        
                        
                        BufferedWriter writeOut = new BufferedWriter(new FileWriter(writingFile,true));
                       
                        writeOut.newLine();
                        writeOut.write("Writing new content to file : " + writingFile);
                        writeOut.flush();
                       
                        ReplicaMain.modifyFileNameVersions("put",nameOfFile,latestVersion+1);
                     
                        System.out.println("After writing to the file");
                        
                        Scanner scannerWrite1 = new Scanner(new File(writingFile));
                        while(scannerWrite1.hasNextLine())
                        {
                            String line1=scannerWrite1.nextLine();
                            
                            if(line1!=null)
                            {
                                System.out.println(line1);
                                
                            }
                        }
                    }
                    if(!ownVotesSuffiecientFlag)
                    {
                        for(Integer member : quoromMembers)
                        {
                            System.out.println("Sending update to " +member);
                            String recipientName=ReplicaMain.indexToNameHashmap.get(member);
                            
                            Socket s=quorumNodeToSocketMap.get(member);
                            ObjectOutputStream updateOut=quorumNodeToOutputStreamMap.get(member);
                            Message msg1=new Message("UPDATE", latestVersion+1,nameOfFile,nodeId,operationName);
                            updateOut.writeObject(msg1);
                        //    System.out.println("Sent update msg");
                           
                            
                        }
                        
                    }
                    
                    lock.setLockValue(false,"shared");
                    //System.out.println("Current value of lock: "+ lock.getLockValue());
                    
                }
                catch(IOException e)
                {
                    System.out.println(e);
                }
                finally
                {
                    
                }
            
            }
            catch(IOException ex)
            {
                Logger.getLogger(GenerateOperations.class.getName()).log(Level.SEVERE, null,ex);
            }
            finally
            {
                try {
                    testOut.close();
                } catch (IOException ex) {
                    Logger.getLogger(GenerateOperations.class.getName()).log(Level.SEVERE, null, ex);
                }
              
            }
	    		
	
	    	 

}
	    	
	   
	    		
	    	
	    }
		
	
		
		
	
