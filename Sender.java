import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
/**
 *
 * @author Bhoomi
 */

public class Sender implements Runnable{

	int nodeId;
	String operationName;
	String nameOfFile;
	int ownVotes;
	Thread t;
	int numberOfVotesSoFar;
	int sleepPeriod;
	
	
	
	
	public HashMap<Integer,Integer> quorumNodeToVersionHashMap =new HashMap<Integer,Integer>();
	
	public HashMap<Integer,Socket> quorumNodeToSocketMap =new HashMap<Integer,Socket>();
        public HashMap<Integer,ObjectOutputStream> quorumNodeToOutputStreamMap =new HashMap<Integer,ObjectOutputStream>();
	
	public Sender(int nodeId, String operationName, String nameOfFile,int ownVotes)
        {
            this.nodeId = nodeId;
            this.operationName=operationName;
            this.nameOfFile=nameOfFile;
            this.ownVotes=ownVotes;
            this.numberOfVotesSoFar=0;


              // ReplicaMain.modifyFileNameVersions(nameOfFile,1);
              // System.out.println("Added file name to fileToVersionMap");
            
            
            /*System.out.println("Printing out the initial key set of quorumNodeToVersionHashMap");
            for(Integer a : quorumNodeToVersionHashMap.keySet())
            {
                System.out.print(a+ " ");
            }*/

            sleepPeriod=1000;	        

            t=new Thread(this);
            t.start();
	    }
	  
	       

    @Override
    public void run()
    {
    boolean votesSufficientFlag=false;
    int numberOfAbortions=0;
    int quorumSize;
    //int tempVotesSoFar;
    BufferedWriter testOut1=null;
    Long time1=null;
            try {
               testOut1 = new BufferedWriter(new FileWriter("Test"+nameOfFile+ReplicaMain.myId+".txt",true));
            } catch (IOException ex) {
                Logger.getLogger(Sender.class.getName()).log(Level.SEVERE, null, ex);
            }
             if(operationName.equalsIgnoreCase("read"))
             {
                  //  System.out.println("this is a read operation");
                     
                     quorumSize=ReplicaMain.r;
             }
             else
             {
                    // System.out.println("this is a write operation");
                quorumSize=ReplicaMain.w;
             }
    
    try {
        
      //  System.out.println(" The name of the file in sender code is :" + nameOfFile);
        Lock lock=ReplicaMain.modifyFileLockMap("Get", nameOfFile,null);
        if(ReplicaMain.fileLockMap.containsKey(nameOfFile))
        {
        //	System.out.println(nameOfFile + " file is present in the map");
        }
        
        
        if(lock.getMode().equalsIgnoreCase("shared"))
        {
           numberOfVotesSoFar=ownVotes;
           
         //  System.out.println("numberOfVotesSoFar=ownVotes  "+numberOfVotesSoFar+"  "+ownVotes);
           
        }
      //  tempVotesSoFar=numberOfVotesSoFar;
        
        if(numberOfVotesSoFar>=quorumSize)
        {
            System.out.println("My own votes are sufficient");
            votesSufficientFlag=true;
            if(operationName.equalsIgnoreCase("write"))   
            {
                lock.setLockValue(true,"exclusive");
                
                 time1 = System.nanoTime();
                   Scanner  Testscannerw = new Scanner(new File("Test"+nameOfFile+ReplicaMain.myId+".txt"));
                 testOut1.newLine();
		 testOut1.write(time1.toString()+" ");
                 
                 System.out.println("LOCKING TIME IN Test"+nameOfFile+ReplicaMain.myId+".txt : "+time1.toString());
                 testOut1.flush();
                 
                
                
                
                
                
                
                
                
                
            }
            else
            {
                System.out.println("I can read with my own votes");
                lock.updateNumberOfGrants(true);
                lock.setLockValue(true,"shared");
                
                time1 = System.nanoTime();
                testOut1.newLine();
                testOut1.write(time1.toString()+" ");
                System.out.println("LOCKING TIME IN Test"+nameOfFile+ReplicaMain.myId+".txt : "+time1.toString());
                testOut1.flush();
              
            }
            operationCaller(ReplicaMain.myId, nameOfFile, null, operationName, null, null, null,true,time1);
          
            
        }
        
        
        
        else
        {

            while(votesSufficientFlag==false && numberOfAbortions < 5)
            {
                System.out.println("Start asking for votes");
                if(lock.getMode().equalsIgnoreCase("shared"))
                {
                numberOfVotesSoFar=ownVotes;
           
                //  System.out.println("numberOfVotesSoFar=ownVotes  "+numberOfVotesSoFar+"  "+ownVotes);
           
                }
                else
                {
                    numberOfVotesSoFar=0;
                }
                System.out.println("votes so far : "+numberOfVotesSoFar);
        
   
             ArrayList<Integer> quoromMembers=new ArrayList<Integer>();
           
           		 
               for(Votes v : ReplicaMain.votesList)
               {
                       int j=v.getNodeId();

                  if(j!=ReplicaMain.myId)
                 {
                   //System.out.println("For quorum member :"  +  j);
                   try {
                           int recipientId=j;
                           String recipientName=ReplicaMain.indexToNameHashmap.get(recipientId);
                           Socket s=new Socket(recipientName,ReplicaMain.porthashmap.get(j));


                           
                           ObjectOutputStream socketOut=null;

                           socketOut = new ObjectOutputStream(s.getOutputStream());
                           
                           ObjectInputStream socketIn=null;

                           socketIn = new ObjectInputStream(s.getInputStream());
                           
                         
                           Integer versionNumber=ReplicaMain.modifyFileNameVersions("get",nameOfFile,null);
                           Message msg=new Message("Request",versionNumber,nameOfFile,ReplicaMain.myId,operationName);

                           //System.out.println("Request for file: " + nameOfFile + " with version number : " + versionNumber + " sent");
                           socketOut.writeObject(msg);
                           System.out.println("Request message to"+operationName+ " "+nameOfFile+" sent to quorum member " + j);

                           
                           Message replyMsg;
                           s.setSoTimeout(3000);
                           try
                           {
                           
                           replyMsg =(Message) socketIn.readObject();
                           }
                           catch(SocketTimeoutException se)
                           {
                               System.out.println("Node  is failed");
                               continue;
                           }


                           if(replyMsg.getType().equalsIgnoreCase("Unlocked"))
                           {
                               quorumNodeToSocketMap.put(j, s);
                               quorumNodeToOutputStreamMap.put(j,socketOut);
                               System.out.println("Member is unlocked ");
                               numberOfVotesSoFar+=ReplicaMain.voteshashmap.get(j);
                               quorumNodeToVersionHashMap.put(replyMsg.getSourceId(), replyMsg.getVersionNumber());
                               System.out.println("After adding "+replyMsg.sourceId+" to key set of quorumNodeToVersionHashMap");
                                for(Integer b : quorumNodeToVersionHashMap.keySet())
                                {
                                    System.out.print(b+ " ");
                                }

                               
                               
                               
                               quoromMembers.add(replyMsg.getSourceId());
                           }
                           if(replyMsg.getType().equalsIgnoreCase("locked"))
                           {
                                   System.out.println("Member locked ");
                                   
                           }
                           if(numberOfVotesSoFar>=quorumSize)
                           {
                                   System.out.println("Number of votes obtained is " + numberOfVotesSoFar);
                                   votesSufficientFlag=true;
                                   break;
                           }
                   } catch (IOException ex) 
                   {
                           Logger.getLogger(Sender.class.getName()).log(Level.SEVERE, null, ex);
                   }
                   catch (ClassNotFoundException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                        }
                       }

               }
	       
               if(votesSufficientFlag)
               {
                   
                   
                  operationCaller(nodeId,nameOfFile,quoromMembers,operationName,quorumNodeToVersionHashMap,quorumNodeToSocketMap,
                           quorumNodeToOutputStreamMap,false,time1);
               }

               else if(!votesSufficientFlag)
               {
                    System.out.println("number of votes obtained so far for "+operationName+" : "+numberOfVotesSoFar);
                  // numberOfVotesSoFar=tempVotesSoFar;
                   int randomSleep= 500 + (int)(Math.random()*sleepPeriod);
                   Thread.sleep(randomSleep);
                   numberOfAbortions++;
                  
                   System.out.println("Aborting");
                   
                   sleepPeriod*=2;
                   
                   for(Integer member : quoromMembers)
                    {
                        System.out.println("Sending Release to " +member);
                        String recipientName=ReplicaMain.indexToNameHashmap.get(member);

                        Socket s=quorumNodeToSocketMap.get(member);
                        ObjectOutputStream updateOut=quorumNodeToOutputStreamMap.get(member);
                        Message msg1=new Message("RELEASE", 999,nameOfFile,nodeId,operationName);
                        updateOut.writeObject(msg1);
                    //    System.out.println("Sent update msg");


                    }
                        
                    
                   
         
                   
               }



               //}
            } 
    
        }
    } catch (InterruptedException e) 
    {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
            } catch (IOException ex) {
                Logger.getLogger(Sender.class.getName()).log(Level.SEVERE, null, ex);
            }
            finally
            {

            }




   }
    
    
    
    public void operationCaller(int nodeId, String nameOfFile,
			ArrayList<Integer> quoromMembers, String operationName, HashMap<Integer,
			Integer> quorumNodeToVersionHashMap, HashMap<Integer, Socket> quorumNodeToSocketMap,
        HashMap<Integer, ObjectOutputStream> quorumNodeToOutputStreamMap,boolean ownVotesSufficient,Long time1) 
    {
        
        
        //System.out.println("Calling performOperation");
                  
        
         GenerateOperations g=new GenerateOperations();
                   System.out.println("Starting the operation");
                   
                   g.performOperation(nodeId,nameOfFile,quoromMembers,operationName,quorumNodeToVersionHashMap,quorumNodeToSocketMap,
                           quorumNodeToOutputStreamMap,ownVotesSufficient,time1);
                   System.out.println("Done with operation");
                   
                   
                   if(ReplicaMain.myId==ReplicaMain.failNode && ReplicaMain.opCount>0)
               {
                   ReplicaMain.failFlag=false;
                   System.out.println("I am failed");
               }
    }



}





