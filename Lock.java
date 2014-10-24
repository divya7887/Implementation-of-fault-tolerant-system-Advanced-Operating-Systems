//package replicamain;
//package trial2;

/**
 *
 * @author Bhoomi
 */
public class Lock
{
    
	public  volatile boolean mylock;
    public  volatile String mode;
    public  volatile int noOfGrants;
    public  volatile String filename;
    
    public Lock(boolean mylock, String mode, int noOfGrants, String filename) {
		
		this.mylock = mylock;
		this.mode = mode;
		this.noOfGrants = noOfGrants;
		this.filename = filename;
	}

	
    
    public  boolean getLockValue()
    {
       
        return mylock;
    }
    
    synchronized void setLockValue(boolean val,String modeVal)
     {
             mylock=val;
             mode=modeVal;
            // System.out.println("Inside setLockValue");
             //System.out.println("Lock set to : "+mylock);
             //System.out.println("mode set to : "+mode);
             
             ReplicaMain.modifyFileLockMap("Put", this.filename, this);
            
         
     }
     public  String getMode()
    {
       
        return mode;
    }
    
    public synchronized void updateNumberOfGrants(boolean unlockedFlag)
    {
        //System.out.println("Inside update no of grants with flag : "+unlockedFlag);
        //System.out.println("current value noOfGrants : "+noOfGrants);
        if(unlockedFlag)
        {
           
            noOfGrants++;
           
        }
        else
        {
            noOfGrants--;
        }
        
        ReplicaMain.modifyFileLockMap("Put", this.filename, this);
        
       // System.out.println("After updating no of grants noOfGrants:"+noOfGrants);
    }

   /* public static synchronized void setGrantsToZero()
    {
        noOfGrants=0;
    }*/
    public int getNumberOfGrants()
    {
        return noOfGrants;
    }

	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}
            


}

