//package replicamain;
//package trial2;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Bhoomi
 */
public class Server implements Runnable{
    
    public int MyPortNo;
    int MyId;
    ServerSocket serversocket;
    Socket mysocket=null;
    Thread t;
   // MyQueue mq;

    public Server(int MyId, int MyPortNo) {
        this.MyId = MyId;
        this.MyPortNo = MyPortNo;
       //this.mq=mq;
        
        t=new Thread(this);
        t.start();
    }
  
        @Override
    public void run()
    {
            
        try {
            serversocket=new ServerSocket(MyPortNo);
        } catch (IOException ex) {
            Logger.getLogger(Server.class.getName()).log(Level.SEVERE, null, ex);
        }
           
          
            while(true)
            {
                try
                {
                   
                    mysocket=serversocket.accept();
                   
                }catch(IOException e)
                {
                    //Step8.logger.info(e.toString());
                }
                if(mysocket != null)
                {
                    ChildThread hc;
                    hc= new ChildThread(mysocket,MyId);
                    
                }
            
            }

    }
    }
    


