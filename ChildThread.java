import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StreamCorruptedException;
import java.net.Socket;
import java.util.Scanner;

/**
 *
 * @author Bhoomi
 */
public class ChildThread implements Runnable
{
    Socket mysocket;
    Thread t1;
    Lock lock;
    int MyId;
    String msg_id;
    int sender;
    int receiver;

    ChildThread(Socket mysocket,int MyId)
    {
            this.MyId=MyId;
            this.mysocket=mysocket;
            
            //lock=new Lock();
            
            //this.mq=mq;
            
            t1=new Thread(this);
            t1.start();
    }
    
   @Override
    public void run()
    {
        
        //System.out.println("In child thread of server");
        Long time1=null;
        boolean unlockFlag=false;
        String reqOperationName=null;
       try
       {
           
	   ObjectInputStream socketIn=null;
		   
	   socketIn = new ObjectInputStream(mysocket.getInputStream());
	   
	   ObjectOutputStream socketOut=null;
	   
	   socketOut = new ObjectOutputStream(mysocket.getOutputStream());
        
	   
          if(ReplicaMain.failFlag==true)
          {
           Message msg =(Message) socketIn.readObject();
          
           String fileName=msg.getFileName();
           
           BufferedWriter testOut = new BufferedWriter(new FileWriter("Test"+fileName+ReplicaMain.myId+".txt",true));

            if(msg.getType().equalsIgnoreCase("Request"))
            {
            	
                 lock=ReplicaMain.modifyFileLockMap("Get", fileName,null);
                 reqOperationName=msg.getOperationName();
            	 boolean lockFlag=lock.getLockValue();
                 String mode=lock.getMode();
                 System.out.println("Request message received for "+reqOperationName+" "+fileName +"from : " + msg.getSourceId());
                
                 Integer versionNumber=ReplicaMain.modifyFileNameVersions("get",fileName,null);
                
                 if(lockFlag==true && ReplicaMain.failFlag==true)
                 {
                     
                     
                     if(msg.getOperationName().equalsIgnoreCase("read"))
                     {
                         //System.out.println("msg.getOperationName().equalsIgnoreCase(read)");
                         
                         if(mode.equalsIgnoreCase("shared"))
                         { 
                            
                             unlockFlag=true;
                            Message unlockedMsg=new Message("Unlocked", versionNumber, fileName, ReplicaMain.myId,"garbage");
                            socketOut.writeObject(unlockedMsg);
                            System.out.println("Sent unlocked message to : " + msg.getSourceId());
                            lock.updateNumberOfGrants(true);
                             
                         }
                         
                         else
                         {
                           
                              //System.out.println("Sending locked message to : " + msg.getSourceId());
                              Message lockedMsg=new Message("Locked", versionNumber, fileName, ReplicaMain.myId,"garbage");
                              System.out.println("Lock parameters :" + lock.getMode() + "  " + lock.getLockValue());
                              socketOut.writeObject(lockedMsg);
                              System.out.println("Sent locked message to : " + msg.getSourceId());
                 
                         }
                         
                         
                         
                     }
                     
                     else
                     {
                                 
                              //System.out.println("Sending locked message to : " + msg.getSourceId());
                              Message lockedMsg=new Message("Locked", versionNumber, fileName, ReplicaMain.myId,"garbage");
                            //  System.out.println("Lock parameters :" + lock.getMode() + " " + lock.getLockValue());
                              socketOut.writeObject(lockedMsg);
                              System.out.println("Sent locked message to : " + msg.getSourceId());
                         
                     }
                	 
                     
                 }
                 else if(ReplicaMain.failFlag==true)
                 {
                      unlockFlag=true;
                      //System.out.println("Sending unlocked message to : " + msg.getSourceId());
                      Message unlockedMsg=new Message("Unlocked", versionNumber, fileName, ReplicaMain.myId,"garbage");
                      socketOut.writeObject(unlockedMsg);
                      System.out.println("Sent unlocked message to : " + msg.getSourceId());
                      
                      if(msg.getOperationName().equalsIgnoreCase("read"))
                      {
                          //System.out.println("Read operation");
                          lock.setLockValue(true, "shared");
                          lock.updateNumberOfGrants(true);
                          
                          
                         //testing code
                         time1 = System.nanoTime();
                         testOut.newLine();
                         testOut.write(time1.toString()+" ");
                        
                         System.out.println("LOCKING TIME IN Test"+fileName+ReplicaMain.myId+".txt : "+time1.toString());
                         testOut.flush();
                     
                      }
                      else
                      {
                          //System.out.println("Write operation");
                          lock.setLockValue(true, "exclusive");
                          
                          //testing code
                         time1 = System.nanoTime();
                         testOut.newLine();
                         testOut.write(time1.toString()+" ");
                         System.out.println("LOCKING TIME IN Test"+fileName+ReplicaMain.myId+".txt : "+time1.toString());
                         testOut.flush();
                        
                      }
                 }
                     
                     
                     
                     
              
                 }
                 
                 if(unlockFlag && ReplicaMain.failFlag==true)
                 { 
                   
                     Message updateMsg =(Message) socketIn.readObject();
                   
                     if(updateMsg.getType().equalsIgnoreCase("Update"))
                    {
                        String readingFile = updateMsg.getFileName() + updateMsg.getSourceId() + ".txt";
                        Scanner scanner = null;
                       
                         int updatedVersionNumber=updateMsg.getVersionNumber();
                         System.out.println("Recieved update message from : " + updateMsg.getSourceId());
                         
       		    
                         if(updateMsg.getOperationName().equalsIgnoreCase("write"))
                        	 
                        {
                        //System.out.println("Inside if for write");
       		 	try 
       		 	{
       		 		scanner = new Scanner(new File(readingFile));
       		 		
       		 	} 
       		 	
       		 	catch (FileNotFoundException e1) 
       		 	{
       		 		// TODO Auto-generated catch block
       		 		e1.printStackTrace();
       		 	}
       		 
       		 	catch (NullPointerException ex)
       		 	{
       		 		
       		 		ex.printStackTrace();
       		 	}         
                 
       		
                            String writingFile= updateMsg.getFileName() + ReplicaMain.myId + ".txt";
                          //  System.out.println("Writing file : "+ writingFile);
                            BufferedWriter updateOut = new BufferedWriter(new FileWriter(writingFile));
                
                        try 
                        {
                          while(scanner.hasNextLine())
                          {
                            //  System.out.println("After while");
                                  String line=scanner.nextLine();

                                  if(line!=null)
                                  {
                                    //System.out.println("Line : "+line);

                                    updateOut.write(line);
                                    updateOut.flush();
                                    updateOut.newLine();

                                  }
                            }



                          //System.out.println("Before fileversion modification"); 
                          ReplicaMain.modifyFileNameVersions("put",updateMsg.getFileName(),updatedVersionNumber);
                          //System.out.println("After fileversion modification");
                        }



                      catch (Exception ex)
                      {
                        System.out.println(ex);
                      } 

                      finally 
                      {


                      } 
                     //System.out.println("Unlocking itself");
                    lock.setLockValue(false,"shared");
                    Scanner unlockScanner=new Scanner(new File("Test"+fileName+ReplicaMain.myId+".txt"));
                    //System.out.println("Before while");
                    while(unlockScanner.hasNextLine())
                    {
                    String line2=unlockScanner.nextLine();
                      //  System.out.println("Line from Test"+fileName+ReplicaMain.myId+".txt : "+line2 );
                        //System.out.println("time1 : "+time1.toString());
                    if(line2!=null && line2.startsWith(time1.toString()))
                    {
                    Long time2 = System.nanoTime();
                    testOut.write(time2.toString()+" "+updateMsg.sourceId+" "+reqOperationName);
                    System.out.println("UNLOCKING TIME IN Test"+fileName+ReplicaMain.myId+".txt :"+ time2.toString());
                    testOut.flush();
                    
                            
                    }
				 
				 
                    }   

        
        
                    }
        
        else
        	 if(updateMsg.getOperationName().equalsIgnoreCase("read"))
                    {
        	       // System.out.println("Inside if for read");
                        lock.updateNumberOfGrants(false);
                        if(lock.getNumberOfGrants()==0)
                        {
                        	
                            lock.setLockValue(false,"shared");
                            Scanner testScanner=new Scanner(new File("Test"+fileName+ReplicaMain.myId+".txt"));
                        while(testScanner.hasNextLine())
                        {
                            String line2=testScanner.nextLine();
                        
                        if(line2!=null && line2.startsWith(time1.toString()))
                        {
                            System.out.println("Time1 : "+time1.toString());
                            Long time2 = System.nanoTime();
                            testOut.write(time2.toString()+" "+updateMsg.sourceId+" "+reqOperationName);
                            System.out.println("UNLOCKING TIME IN Test"+fileName+ReplicaMain.myId+".txt :"+ time2.toString());
                            testOut.flush();
                            
                            
                        }
				 
				 
                        }
                        }
                        
                      
                        
                        System.out.println("Done modifying for read");
                    
                    }
        	
         }
                     
                     
                     
                     else if(updateMsg.getType().equalsIgnoreCase("RELEASE"))
                     {
                        System.out.println("Recieved release message from : " + updateMsg.getSourceId());
                         if(updateMsg.getOperationName().equalsIgnoreCase("write"))
                         {
                            lock.setLockValue(false,"shared");
                            Scanner abortUnlockScanner=new Scanner(new File("Test"+fileName+ReplicaMain.myId+".txt"));
                            //System.out.println("Before while");
                            while(abortUnlockScanner.hasNextLine())
                            {
                            String line3=abortUnlockScanner.nextLine();
                              //  System.out.println("Line from Test"+fileName+ReplicaMain.myId+".txt : "+line2 );
                                //System.out.println("time1 : "+time1.toString());
                            if(line3!=null && line3.startsWith(time1.toString()))
                            {
                            Long time3 = System.nanoTime();
                            testOut.write(time3.toString()+" "+updateMsg.sourceId+" "+reqOperationName);
                            System.out.println("UNLOCKING TIME IN Test"+fileName+ReplicaMain.myId+".txt :"+ time3.toString());
                            testOut.flush();

                            }

                            }   

                        }
                        else if(updateMsg.getOperationName().equalsIgnoreCase("read"))
                        {
                            lock.updateNumberOfGrants(false);
                            if(lock.getNumberOfGrants()==0)
                            {

                                lock.setLockValue(false,"shared");
                                Scanner abortTestScanner=new Scanner(new File("Test"+fileName+ReplicaMain.myId+".txt"));
                                while(abortTestScanner.hasNextLine())
                                {
                                String line3=abortTestScanner.nextLine();

                                    if(line3!=null && line3.startsWith(time1.toString()))
                                    {
                                        System.out.println("Time1 : "+time1.toString());
                                        Long time3 = System.nanoTime();
                                        testOut.write(time3.toString()+" "+updateMsg.sourceId+" "+reqOperationName);
                                        System.out.println("UNLOCKING TIME IN Test"+fileName+ReplicaMain.myId+".txt :"+ time3.toString());
                                        testOut.flush();


                                    }


                                }
                            }
                        
                      
                        } 
                         
                         
                         
                         
                         
                        }
                     
       }//End of if unlock
       }
       }//End of try
                 catch(StreamCorruptedException e)
       {
           System.out.println(" Reason for Stream corruption : " + e.getMessage());
           
       }
       
       
       catch (Exception ex)
       {
         System.out.println(ex);
       } 
            
       finally 
       {
       
            
       }      
       
       }
            }
       
        
            
            
            
            
            
            
   
          

