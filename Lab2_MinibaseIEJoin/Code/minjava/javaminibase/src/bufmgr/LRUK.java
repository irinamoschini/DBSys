/* File LRUK.java */

package bufmgr;

import diskmgr.*;
import global.*;

  /**
   * class LRU-K is a subclass of class Replacer using LRU-K
   * algorithm for page replacement
   */
public class LRUK extends  Replacer {

	/**
   * private field
   * An array to hold number of frames in the buffer pool
   */
   private int  frames[];
	 

  /**
   * private field
   * number of frames used
   */   
   private int  nframes;
  
  /**
   * private field
   * An array to hold the last K occurrences of each frame in the buffer pool
   */

    private long  hist[][];
    
    /**
     * private field
     * An array to hold the last occurrence of each frame in the buffer pool
     */

    private long  last[];
    
    /**
     * private field
     * Last reference k.
     */

    private int lastRef;

  /**
   * This updates the hist and last lists for the frame FrameNo in the buffer pool
   * @param frameNo	the frame number
   */
  private void update(int frameNo)
  {
     int index;
     int j;
     int lastRef = mgr.getLastRef();

	 //System.out.println("update begins");

     for ( index=1; index < lastRef ; ++index ) {
         hist[frameNo][index] = hist[frameNo][index-1];

     }
     
     hist[frameNo][0] = java.lang.System.currentTimeMillis();
     last[frameNo]=hist[frameNo][0];

     for ( j = 0; j < nframes; ++j )
         if ( frames[j] == frameNo )
             break;

     while ( ++j < nframes -1 )
         frames[j-1] = frames[j];
         frames[nframes-1] = frameNo;
      

  }


/**
   * Calling super class the same method
   * The hist[][] is a list of lists that contains the frames in the buffer pool and
   * the k last times when they had been called (size numBuffer, LastRef)
   * set hist list to zero as no frame is used at the beginning
   * set number of frame used to zero
   *
   * @param	mgr	a BufMgr object
   * @see	BufMgr
   * @see	Replacer
   */
    public void setBufferManager( BufMgr mgr )
     {
        super.setBufferManager(mgr);
        int lastRef = mgr.getLastRef();
        frames = new int [ mgr.getNumBuffers() ];
        hist = new long [ mgr.getNumBuffers() ][ lastRef ];
        last = new long [ mgr.getNumBuffers() ];
       
        nframes = 0;
     }
    
    
/* public methods */

  /**
   * Class constructor
   * Initializing frames[] pinter = null.
   */
    public LRUK(BufMgr mgrArg)
    {
      super(mgrArg);
      last = null;
      hist = null; 
      frames = null;

    }
  
  /**
   * call super class the same method
   * pin the page in the given frame number 
   * update the hist and last lists
   *
   * @param	 frameNo	 the frame number to pin
   * @exception  InvalidFrameNumberException
   */
 public void pin(int frameNo) throws InvalidFrameNumberException
 {
    super.pin(frameNo);
 

  /**
   * SYSTEMATIC UPDATE HERE OF THE HIST LIST FOR ALL PIN
   */  
    update(frameNo);
    
 }

  /**
   * Finding a free frame in the buffer pool if enough place in it
   * or choosing a page to replace using LRU-K policy
   *
   * @return 	return the frame number
   *		return -1 if failed
   */

 public int pick_victim() throws BufferPoolExceededException
 {
   int numBuffers = mgr.getNumBuffers();
   int frame;
   long min = java.lang.System.currentTimeMillis();
   int victim = -1;
   int lastRef = mgr.getLastRef();

   //System.out.println("          ");
   //System.out.println("NUMBER OF FRAMES IN THE BUFFER:" + nframes);


  /**
   * There is enough space in the buffer pool: the victim is one of the free frame
   */
    if ( nframes < numBuffers ) {
    	
        frame = nframes;
        nframes++;
        
        for(int i=1 ; i<lastRef;i++) {
     	   hist[frame][i]=0;
     	}
        hist[frame][0]=min;
        //System.out.println("hist = " + hist[frame][0]);

        
        //frames[frame]=frame;
        
        state_bit[frame].state = Pinned;
        (mgr.frameTable())[frame].pin();
  
        //System.out.println("ENOUGH SPACE IN THE BUFFER POOL ");
        return frame;
    }


      for ( int i = 0; i < numBuffers ; ++i ) {
          
  /**
   * If the buffer pool is full but one page had NOT been called 
   * at least k times: choose it as the victim
   */
    	  //System.out.println("BUFFER POOL FULL");
    	  if (hist[i][lastRef-1] == 0 && state_bit[i].state != Pinned) {
    		  victim = i; 
    	  	  break;
    	  }

  /**
   * If the buffer pool is full and all pages had been called 
   * at least k times: apply the LRU-K policy to choose the victim
   */
    	  else if ( hist[i][lastRef-1] <= min && state_bit[i].state != Pinned ) {
        	victim = i;
            min = hist[i][lastRef-1];
        }
    	    
    }
 
      if (victim != -1)
      {
        state_bit[victim].state = Pinned;
  	    (mgr.frameTable())[victim].pin();
  	    update(victim);
  	    //System.out.println(victim);
  	    return victim;              

      }
    
   System.out.println("no victim found");

   throw new BufferPoolExceededException (null, "BUFMGR: BUFFER_EXCEEDED.");
   
    
 }
 
  /**
   * get the page replacement policy name
   *
   * @return	return the name of replacement policy used
   */  
    public String name() { return "LRUK"; }
 
  /**
   * print out the information of frame usage
   */  
 public void info()
 {
    super.info();

    System.out.print( "LRUK REPLACEMENT");
    
    for (int i = 0; i < nframes; i++) {
        if (i % 5 == 0)
	System.out.println( );
	System.out.print( "\t" + i);
        
    }
    System.out.println();
 }

public int[] getFrames() {
	return this.frames;
}

public long last(int pagenumber) {
	return last[pagenumber];
}

public long hist(int pagenumber, int i) {
	return hist[pagenumber][i];
}
  
}



