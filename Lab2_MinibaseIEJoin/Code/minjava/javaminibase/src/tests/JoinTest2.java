package tests;
//originally from : joins.C

import iterator.*;
import heap.*;
import global.*;
import index.*;
import java.io.*;
import java.util.*;
import java.lang.*;
import diskmgr.*;
import bufmgr.*;
import btree.*; 
import catalog.*;

/**
   Here is the implementation for the tests. There are N tests performed.
   We start off by showing that each operator works on its own.
   Then more complicated trees are constructed.
   As a nice feature, we allow the user to specify a selection condition.
   We also allow the user to hardwire trees together.
*/

//Define the Table2 schema
class Table1 {
  public int col1;
  public int col2;
  public int col3;
  public int col4;
  ;
  public Table1 (int _col1, int _col2, int _col3, int _col4) {
    col1 = _col1;
    col2 = _col2;
    col3 = _col3;
    col4 = _col4;
  }
}
 
//Define the Table2 schema
class Table2 {
  public int col1;
  public int col2;
  public int col3;
  public int col4;
  

  public Table2 (int _col1, int _col2, int _col3, int _col4) {
    col1 = _col1;
    col2 = _col2;
    col3 = _col3;
    col4 = _col4;
  }
}


class JoinsDriver2 implements GlobalConst{
  
  private boolean OK = true;
  private boolean FAIL = false;
  private Vector table1; 
  private Vector table2; 

	public JoinsDriver2(String pathTable1) {
		
		table1 = new Vector(); 
		
	    String[] arrayR ;
	    String[] arrayS ;
		
	    //building table Table1 given the file Table1.txt we read
	  try{
	    File fileR = new File(pathTable1);
	    Scanner scannerR = new Scanner(fileR);
	    scannerR.nextLine();
	    while (scannerR.hasNextLine()){
	       String line = scannerR.nextLine(); 
	       String[] lineWords = line.split(",");
	       int r1 = Integer.parseInt(lineWords[0]);
	       int r2 = Integer.parseInt(lineWords[1]);
	       int r3 = Integer.parseInt(lineWords[2]);
	       int r4 = Integer.parseInt(lineWords[3]);
	       table1.addElement(new Table1(r1, r2, r3,r4));
	    }
	  } catch (FileNotFoundException e){
	      System.out.println("File given not found.");
	  }
	

	    // Initial size setting to accomodate the entire dataset
	    // Each relation admits 4 attributes 
	
	    boolean status = OK;
	    int numr = table1.size();
	    int numr_attrs = 4;	
	
	    // initialization of k, a parameter that is needed
	
	    int k = 2;
	    
	    String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.jointestdb"; 
	    String logpath = "/tmp/"+System.getProperty("user.name")+".joinlog";
	
	    String remove_cmd = "/bin/rm -rf ";
	    String remove_logcmd = remove_cmd + logpath;
	    String remove_dbcmd = remove_cmd + dbpath;
	    String remove_joincmd = remove_cmd + dbpath;
	
	    try {
	      Runtime.getRuntime().exec(remove_logcmd);
	      Runtime.getRuntime().exec(remove_dbcmd);
	      Runtime.getRuntime().exec(remove_joincmd);
	    }
	    catch (IOException e) {
	      System.err.println (""+e);
	    }
	
	   
	    /*
	    ExtendedSystemDefs extSysDef = 
	      new ExtendedSystemDefs( "/tmp/minibase.jointestdb", "/tmp/joinlog",
				      1000,500,200,"Clock");
	    */
	
	    SystemDefs sysdef = new SystemDefs( dbpath, 1000, NUMBUF, "Clock", k);
	    	    
	    //creating the Table1 relation
	    AttrType [] Rtypes = new AttrType[4];
	    Rtypes[0] =  new AttrType(AttrType.attrInteger);
	    Rtypes[1] =  new AttrType(AttrType.attrInteger);
	    Rtypes[2] =  new AttrType(AttrType.attrInteger);
	    Rtypes[3] =  new AttrType(AttrType.attrInteger); 
	  
	    Tuple t = new Tuple();
	    short  []  Rsizes = new short[1];
	    Rsizes[0] = 0;
	    t = new Tuple();
	    try {
	      t.setHdr((short) 4,Rtypes, Rsizes);
	    }
	    catch (Exception e) {
	      System.err.println("*** error in Tuple.setHdr() ***");
	      status = FAIL;
	      e.printStackTrace();
	    }
	    
	    int size = t.size();
	    size = t.size();
	    
	    // inserting the tuple into file Table1
	    //RID             rid;
	    RID             rid;
	    Heapfile        f = null;
	    f = null;
	    try {
	      f = new Heapfile("table1.in");
	    }
	    catch (Exception e) {
	      System.err.println("*** error in Heapfile constructor ***");
	      status = FAIL;
	      e.printStackTrace();
	    }
	    
	    t = new Tuple(size);
	    try {
	      t.setHdr((short) 4, Rtypes, Rsizes);
	    }
	    catch (Exception e) {
	      System.err.println("*** error in Tuple.setHdr() ***");
	      status = FAIL;
	      e.printStackTrace();
	    }
	    
	    for (int i=0; i<numr; i++) {
	      try {
		t.setIntFld(1, ((Table1)table1.elementAt(i)).col1);
		t.setIntFld(2, ((Table1)table1.elementAt(i)).col2);
		t.setIntFld(3, ((Table1)table1.elementAt(i)).col3);
		t.setIntFld(4, ((Table1)table1.elementAt(i)).col4);
	      }
	      catch (Exception e) {
		System.err.println("*** error in Tuple.setStrFld() ***");
		status = FAIL;
		e.printStackTrace();
	      }
	      
	      try {
		rid = f.insertRecord(t.returnTupleByteArray());
	      }
	      catch (Exception e) {
		System.err.println("*** error in Heapfile.insertRecord() ***");
		status = FAIL;
		e.printStackTrace();
	      }      
	    }
	    if (status != OK) {
	      //bail out
	      System.err.println ("*** Error creating relation for Table1");
	      Runtime.getRuntime().exit(1);
	    }
	  }

	
	
	public JoinsDriver2(String pathTable1, String pathTable2) {
		
		table1 = new Vector(); 
		table2 = new Vector(); 
		
	    String[] arrayR ;
	    String[] arrayS ;
		
	    //building table Table1 given the file Table1.txt we read
	  try{
	    File fileR = new File(pathTable1);
	    Scanner scannerR = new Scanner(fileR);
	    scannerR.nextLine();
	    while (scannerR.hasNextLine()){
	       String line = scannerR.nextLine(); 
	       String[] lineWords = line.split(",");
	       int r1 = Integer.parseInt(lineWords[0]);
	       int r2 = Integer.parseInt(lineWords[1]);
	       int r3 = Integer.parseInt(lineWords[2]);
	       int r4 = Integer.parseInt(lineWords[3]);
	       table1.addElement(new Table1(r1, r2, r3,r4));
	    }
	  } catch (FileNotFoundException e){
	      System.out.println("File given not found.");
	  }
	
	    //building table Table2 given the file Table2.txt we read
	  try{
	    File fileS = new File(pathTable2);
	    Scanner scannerS = new Scanner(fileS);
	    scannerS.nextLine();
	    while (scannerS.hasNextLine()){
	       String line = scannerS.nextLine(); 
	       String[] lineWords = line.split(",");
	       int s1 = Integer.parseInt(lineWords[0]);
	       int s2 = Integer.parseInt(lineWords[1]);
	       int s3 = Integer.parseInt(lineWords[2]);
	       int s4 = Integer.parseInt(lineWords[3]);
	       table2.addElement(new Table2(s1, s2, s3,s4));
	    }
	  } catch (FileNotFoundException e){
	      System.out.println("File given not found.");
	  }
	  
	  
	    // Initial size setting to accomodate the entire dataset
	    // Each relation admits 4 attributes 
	
	    boolean status = OK;
	    int nums = table2.size();
	    int nums_attrs = 4;
	    int numr = table1.size();
	    int numr_attrs = 4;	
	
	    // initialization of k, a parameter that is needed
	
	    int k = 2;
	    
	    String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.jointestdb"; 
	    String logpath = "/tmp/"+System.getProperty("user.name")+".joinlog";
	
	    String remove_cmd = "/bin/rm -rf ";
	    String remove_logcmd = remove_cmd + logpath;
	    String remove_dbcmd = remove_cmd + dbpath;
	    String remove_joincmd = remove_cmd + dbpath;
	
	    try {
	      Runtime.getRuntime().exec(remove_logcmd);
	      Runtime.getRuntime().exec(remove_dbcmd);
	      Runtime.getRuntime().exec(remove_joincmd);
	    }
	    catch (IOException e) {
	      System.err.println (""+e);
	    }
	
	   
	    /*
	    ExtendedSystemDefs extSysDef = 
	      new ExtendedSystemDefs( "/tmp/minibase.jointestdb", "/tmp/joinlog",
				      1000,500,200,"Clock");
	    */
	
	    SystemDefs sysdef = new SystemDefs( dbpath, 1000, NUMBUF, "Clock", k);
	    
	    // creating the Table2 relation
	    AttrType [] Stypes = new AttrType[4];
	    Stypes[0] = new AttrType (AttrType.attrInteger);
	    Stypes[1] = new AttrType (AttrType.attrInteger);
	    Stypes[2] = new AttrType (AttrType.attrInteger);
	    Stypes[3] = new AttrType (AttrType.attrInteger);
	
	    //SOS
	    short [] Ssizes = new short [1];
	    Ssizes[0] = 0; //first elt. is empty
	    
	    Tuple t = new Tuple();
	    try {
	      t.setHdr((short) 4,Stypes, Ssizes);
	    }
	    catch (Exception e) {
	      System.err.println("*** error in Tuple.setHdr() ***");
	      status = FAIL;
	      e.printStackTrace();
	    }
	    
	    int size = t.size();
	    
	    // inserting the tuple into file "Table2"
	    RID             rid;
	    Heapfile        f = null;
	    try {
	      f = new Heapfile("table2.in");
	    }
	    catch (Exception e) {
	      System.err.println("*** error in Heapfile constructor ***");
	      status = FAIL;
	      e.printStackTrace();
	    }
	    
	    t = new Tuple(size);
	    try {
	      t.setHdr((short) 4, Stypes, Ssizes);
	    }
	    catch (Exception e) {
	      System.err.println("*** error in Tuple.setHdr() ***");
	      status = FAIL;
	      e.printStackTrace();
	    }
	    
	    for (int i=0; i<nums; i++) {
	      try {
		t.setIntFld(1, ((Table2)table2.elementAt(i)).col1);
		t.setIntFld(2, ((Table2)table2.elementAt(i)).col2);
		t.setIntFld(3, ((Table2)table2.elementAt(i)).col3);
		t.setIntFld(4, ((Table2)table2.elementAt(i)).col4);
	      }
	      catch (Exception e) {
		System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
		status = FAIL;
		e.printStackTrace();
	      }
	      
	      try {
		rid = f.insertRecord(t.returnTupleByteArray());
	      }
	      catch (Exception e) {
		System.err.println("*** error in Heapfile.insertRecord() ***");
		status = FAIL;
		e.printStackTrace();
	      }      
	    }
	    if (status != OK) {
	      //bail out
	      System.err.println ("*** Error creating relation for Table2");
	      Runtime.getRuntime().exit(1);
	    }
	    
	    //creating the Table1 relation
	    AttrType [] Rtypes = new AttrType[4];
	    Rtypes[0] =  new AttrType(AttrType.attrInteger);
	    Rtypes[1] =  new AttrType(AttrType.attrInteger);
	    Rtypes[2] =  new AttrType(AttrType.attrInteger);
	    Rtypes[3] =  new AttrType(AttrType.attrInteger); 
	  
	    
	    short  []  Rsizes = new short[1];
	    Rsizes[0] = 0;
	    t = new Tuple();
	    try {
	      t.setHdr((short) 4,Rtypes, Rsizes);
	    }
	    catch (Exception e) {
	      System.err.println("*** error in Tuple.setHdr() ***");
	      status = FAIL;
	      e.printStackTrace();
	    }
	    
	    size = t.size();
	    
	    // inserting the tuple into file Table1
	    //RID             rid;
	    f = null;
	    try {
	      f = new Heapfile("table1.in");
	    }
	    catch (Exception e) {
	      System.err.println("*** error in Heapfile constructor ***");
	      status = FAIL;
	      e.printStackTrace();
	    }
	    
	    t = new Tuple(size);
	    try {
	      t.setHdr((short) 4, Rtypes, Rsizes);
	    }
	    catch (Exception e) {
	      System.err.println("*** error in Tuple.setHdr() ***");
	      status = FAIL;
	      e.printStackTrace();
	    }
	    
	    for (int i=0; i<numr; i++) {
	      try {
		t.setIntFld(1, ((Table1)table1.elementAt(i)).col1);
		t.setIntFld(2, ((Table1)table1.elementAt(i)).col2);
		t.setIntFld(3, ((Table1)table1.elementAt(i)).col3);
		t.setIntFld(4, ((Table1)table1.elementAt(i)).col4);
	      }
	      catch (Exception e) {
		System.err.println("*** error in Tuple.setStrFld() ***");
		status = FAIL;
		e.printStackTrace();
	      }
	      
	      try {
		rid = f.insertRecord(t.returnTupleByteArray());
	      }
	      catch (Exception e) {
		System.err.println("*** error in Heapfile.insertRecord() ***");
		status = FAIL;
		e.printStackTrace();
	      }      
	    }
	    if (status != OK) {
	      //bail out
	      System.err.println ("*** Error creating relation for Table1");
	      Runtime.getRuntime().exit(1);
	    }
	  }


  public boolean runTests() {
    
	  
	/*
	 * 
	 *Try Query1a() and Query1b() separately
	 *
	 */
    Disclaimer();
    //Query1a();
    //Query1b();

    System.out.print ("Finished joins testing"+"\n");
   
    
    return true;
  }
  
  public boolean runTests2(String pathFile, int query) {
    
	  String[] array ;
	  int output_rel1 = 0;
	  String outputTable1 = null; 
	  int output_rel2 = 0 ;
	  String outputTable2 = null; 
	  
	  int nbOfTable = 0;
	  String table1 = null; 
	  String table2 = null; 

	  int cond_rel1 = 0;
	  String condTable1 = null; 
	  int cond_rel2 = 0 ;
	  String condTable2 = null; 
	  int op = 0;

	  
	  String cond1Table1 = null; 
	  int cond1_rel1 = 0;
	  int op1 = 0;
	  String cond1Table2 = null; 
	  int cond1_rel2 = 0 ;
	  String conjonction = null;
	  String cond2Table1 = null; 
	  int cond2_rel1 = 0;
	  int op2 = 0;
	  String cond2Table2 = null; 
	  int cond2_rel2 = 0 ;

	  
	  
   	  int lines = 0; 
	    try{
	    	InputStream flux = new FileInputStream(pathFile); 
	    	InputStreamReader text = new InputStreamReader(flux);
	    	BufferedReader buff = new BufferedReader(text);
	    	while (buff.readLine() != null) lines++;
    	buff.close(); 
    	}		
    	catch (Exception e){
    	System.out.println(e.toString());
    	}

	    
	    /*
	     * 
	     * If the number of lines in the query is 3, 2 algorithms can be called: 
	     * 		- NestedLoopsJoins for a single predicate
	     * 		- SelfJoin for a single predicate
	     * To know which one has to be called, we scan the line 2 to determine how 
	     * many tables are used (if a single table is used, call SelfJoin, if 
	     * 2 tables are used, call NestedLoopsJoins)
	     * 
	     */
	    if (lines == 3) {
	  	    try{
	  	    	InputStream flux = new FileInputStream(pathFile); 
	  	    	InputStreamReader text = new InputStreamReader(flux);
	  	    	BufferedReader buff = new BufferedReader(text);
	  	    	String line;
	  	    	int line_number = 1;
	  	    	while ((line = buff.readLine())!= null){ 
	  	    		if (line_number == 1) { 
	  		    		array = line.split(" ") ;
	  		    		outputTable1 = String.valueOf(array[0].split("_")[0]) ;
	  		    		output_rel1 = Integer.valueOf(array[0].split("_")[1]) ;
	  		    		outputTable2 = String.valueOf(array[1].split("_")[0]) ;
	  		    		output_rel2 = Integer.valueOf(array[1].split("_")[1]) ;
	  	    		}
	  	    		if (line_number == 2) {
	  		    		array = line.split(" ") ;
	  		    		nbOfTable = array.length;
	  		    		table1 = String.valueOf(array[0].split("_")[0]) ;
	  		    		if (nbOfTable == 2) {table2 = String.valueOf(array[1].split("_")[0]) ;}
	  	    		}
	  	    		if (line_number == 3) { 
	  		    		array = line.split(" ");
	  		    		condTable1 = String.valueOf(array[0].split("_")[0]) ;
	  		    		cond_rel1 = Integer.valueOf(array[0].split("_")[1]) ;
	  		    		op = Integer.valueOf(array[1]);
	  		    		condTable2 = String.valueOf(array[2].split("_")[0]) ;
	  		    		cond_rel2 = Integer.valueOf(array[2].split("_")[1]) ;
	  	    		}
	  	    		line_number+=1;
	  	    	}
	  	    	if (nbOfTable == 2) {
			  	    System.out.print("**********************Query1a being read *********************\n");
			  	    System.out.print ("Query: \n"
			  			      + "  SELECT " + outputTable1 + ".col" + output_rel1 + " " + outputTable2 + ".col" + output_rel2 + "\n"
			  			      + "  FROM   " + table1+ ", " + table2+ "\n"
			  			      + "  WHERE  " + condTable1 + ".col" + cond_rel1 + " " +op + " " + condTable2 +".col" + cond_rel2 + "\n\n");

			  	    
		  	    	buff.close(); 
		  		    Disclaimer();
		  			Query1a(output_rel1,output_rel2,cond_rel1,cond_rel2,op);
	  	    	}
	  	    	else if (nbOfTable == 1){
			  	    System.out.print("**********************Query2a being read *********************\n");
			  	    System.out.print ("Query: \n"
			  			      + "  SELECT " + outputTable1 + ".col" + output_rel1 + " " + outputTable2 + ".col" + output_rel2 + "\n"
			  			      + "  FROM   " + table1 + "\n"
			  			      + "  WHERE  " + condTable1 + ".col" + cond_rel1 + " " +op + " " + condTable2 +".col" + cond_rel2 + "\n\n");

		  	    	buff.close(); 
		  		    Disclaimer();
		  		    Query2a(output_rel1,output_rel2,cond_rel1,cond_rel2,op);
	  	    	}
	  	    	else {
			  	    System.out.print("Asked to work on too many tables");
	  	    	}
	  	    	}		
	  	    	catch (Exception e){
	  	    	System.out.println(e.toString());
	  	    	}	    	
	    }
	    
	    
	    
	    /*
	     * 
	     * If the number of lines in the query is 5, 2 algorithms can be called: 
	     * 		- NestedLoopsJoins for two predicates
	     * 		- SelfJoin for two predicate
	     * To know which one has to be called, we scan the line 2 to determine how 
	     * many tables are used (if a single table is used, call SelfJoin2, if 
	     * 2 tables are used, call NestedLoopsJoins for two predicates)
	     * 
	     */
	    else if (lines == 5){
	    	
	    		
	    	
	  	  /* 
	  	   * Query 1b, query 2b and query 2c can be called
	  	   * Query 2b is called if a single table is used
	  	   * If 2 tables are used, we can use query 1b or query 2c
	  	   */
	  	    try{
	  	    	InputStream flux = new FileInputStream(pathFile); 
	  	    	InputStreamReader text = new InputStreamReader(flux);
	  	    	BufferedReader buff = new BufferedReader(text);
	  	    	String line;
	  	    	int line_number = 1;
	  	    	while ((line = buff.readLine())!= null){ 
	  	    		if (line_number == 1) { 
	  		    		array = line.split(" ") ;
	  		    		outputTable1 = String.valueOf(array[0].split("_")[0]) ;
	  		    		output_rel1 = Integer.valueOf(array[0].split("_")[1]) ;
	  		    		outputTable2 = String.valueOf(array[1].split("_")[0]) ;
	  		    		output_rel2 = Integer.valueOf(array[1].split("_")[1]) ;
	  	    		}
	  	    		if (line_number == 2) {
	  		    		array = line.split(" ") ;
	  		    		nbOfTable = array.length;
	  		    		table1 = String.valueOf(array[0].split("_")[0]) ;
	  		    		if (nbOfTable == 2) {table2 = String.valueOf(array[1].split("_")[0]) ;}
	  	    		}
	  	    		if (line_number == 3) { 
	  		    		array = line.split(" ");
	  		    		cond1Table1 = String.valueOf(array[0].split("_")[0]) ;
	  		    		cond1_rel1 = Integer.valueOf(array[0].split("_")[1]) ;
	  		    		op1 = Integer.valueOf(array[1]);
	  		    		cond1Table2 = String.valueOf(array[2].split("_")[0]) ;
	  		    		cond1_rel2 = Integer.valueOf(array[2].split("_")[1]) ;
	  	    		}
	  	    		if (line_number == 4) { 
	  		    		conjonction = line; ;
	  	    		}

	  	    		if (line_number == 5) { 
	  		    		array = line.split(" ") ;
	  		    		cond2Table1 = String.valueOf(array[0].split("_")[0]) ;
	  		    		cond2_rel1 = Integer.valueOf(array[0].split("_")[1]) ;
	  		    		op2 = Integer.valueOf(array[1]);
	  		    		cond2Table2 = String.valueOf(array[2].split("_")[0]) ;
	  		    		cond2_rel2 = Integer.valueOf(array[2].split("_")[1]) ;
	  	    		}
	  	    		line_number+=1;
	  	    	}
	  	    	if (nbOfTable == 2) {
	  	    		
	  	    		if (query == 1) {
	  	    		
			  	    System.out.print("**********************Query1b being read *********************\n");
			  	    System.out.print ("Query: \n"
			  			      + "  SELECT " + outputTable1 + ".col" + output_rel1 + " " + outputTable2 + ".col" + output_rel2 + "\n"
			  			      + "  FROM   " + table1+ ", " + table2+ "\n"
			  			      + "  WHERE  " + cond1Table1 + ".col" + cond1_rel1 + " " + op1 + " " + cond1Table2 +".col" + cond1_rel2 + "\n"
				      		  +  "         " + conjonction + "   " + cond2Table1 + ".col" + cond2_rel1 + " " + op2 + " " + cond2Table2 +".col" + cond2_rel2+ "\n\n");

			  	    
		  	    	buff.close(); 
		  		    Disclaimer();
		  		    Query1b(output_rel1,output_rel2,cond1_rel1,cond1_rel2,op1, cond2_rel1, cond2_rel2, op2, conjonction);
		  		    }
	  	    		else {
	  	    		
			  	    System.out.print("**********************Query2c being read *********************\n");
			  	    System.out.print ("Query: \n"
			  			      + "  SELECT " + outputTable1 + ".col" + output_rel1 + " " + outputTable2 + ".col" + output_rel2 + "\n"
			  			      + "  FROM   " + table1 + ", " + table2+ "\n"
			  			      + "  WHERE  " + cond1Table1 + ".col" + cond1_rel1 + " " + op1 + " " + cond1Table2 +".col" + cond1_rel2 + "\n"
				      		  +  "         " + conjonction +"   " + cond2Table1 + ".col" + cond2_rel1 + " " + op2 + "    " + cond2Table2 + ".col" + cond2_rel2 + "\n\n");
			  	    
		  	    	buff.close(); 
		  		    Disclaimer();
		  		    
		  		    Query2c(output_rel1,output_rel2,cond1_rel1,cond1_rel2,op1, cond2_rel1, cond2_rel2, op2, conjonction);

	  	    		}
	  	    	}
	  	    	else if (nbOfTable == 1){
			  	    System.out.print("**********************Query2b being read *********************\n");
			  	    System.out.print ("Query: \n"
			  			      + "  SELECT " + outputTable1 + ".col" + output_rel1 + " " + outputTable2 + ".col" + output_rel2 + "\n"
			  			      + "  FROM   " + table1 + "\n"
			  			      + "  WHERE  " + cond1Table1 + ".col" + cond1_rel1 + " " + op1 + " " + cond1Table2 +".col" + cond1_rel2 + "\n"
				      		  +  "         " + conjonction + "   Table1.col" + cond2_rel1 + " " + op2 + " Table2.col" + cond2_rel2 + "\n\n");

		  	    	buff.close(); 
		  		    Disclaimer();
		  		    Query2b(output_rel1,output_rel2,cond1_rel1,cond1_rel2,op1, cond2_rel1, cond2_rel2, op2, conjonction);
	  	    	}
	  	    	else {
			  	    System.out.print("Asked to work on too many tables");
	  	    	}
	  	    	}
	  	    	catch (Exception e){
	  	    	System.out.println(e.toString());
	  	    	}	    	
	    }  

	    
	    System.out.print ("Finished joins testing"+"\n");
		   
		    
		    return true;
			}
  

  private void Query1a_CondExpr(CondExpr[] expr, int output_rel1,
  int output_rel2,
  int cond_rel1,
  int cond_rel2,
  int op) {

	expr[0].next  = null;
    // We test the "lower than" inequality join
    expr[0].op    = new AttrOperator(op);
    expr[0].type1 = new AttrType(AttrType.attrSymbol);
    expr[0].type2 = new AttrType(AttrType.attrSymbol);
    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),cond_rel1);
    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),cond_rel2);
    
 
    expr[1] = null;
  }


  private void Query1b_CondExpr(CondExpr[] expr, int output_rel1, int output_rel2, int cond1_rel1, 
		  int cond1_rel2, int op1, int cond2_rel1, int cond2_rel2, int op2, String conjonction) {

	String and = new String("AND"); 
	String or = new String("OR"); 

	if (conjonction.equalsIgnoreCase(and)) {
	    expr[0].next  = null;
	    expr[0].op    = new AttrOperator(op1);
	    expr[0].type1 = new AttrType(AttrType.attrSymbol);
	    expr[0].type2 = new AttrType(AttrType.attrSymbol);
	    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),cond1_rel1);
	    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),cond1_rel2);
	
	    expr[1].op    = new AttrOperator(op2);
	    expr[1].next  = null;
	    expr[1].type1 = new AttrType(AttrType.attrSymbol);
	    expr[1].type2 = new AttrType(AttrType.attrSymbol);
	    /// ATTENTION 
	    expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer), cond2_rel1);
	    expr[1].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),cond2_rel2);
	 
	    expr[2] = null;
	}
	else if (conjonction.equals(or)) {	
	    expr[0].next = null;
	    expr[0].op   = new AttrOperator(op1);
	    expr[0].type1 = new AttrType(AttrType.attrSymbol);
	    expr[0].type2 = new AttrType(AttrType.attrSymbol);
	    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),cond1_rel1);
	    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),cond1_rel2);

	    expr[0].next = new CondExpr();
	    expr[0].next.op   = new AttrOperator(op2);
	    expr[0].next.type1 = new AttrType(AttrType.attrSymbol);
	    expr[0].next.type2 = new AttrType(AttrType.attrSymbol);
	    expr[0].next.operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),cond2_rel1);
	    expr[0].next.operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),cond2_rel2);
	    expr[0].next.next = null;

	    expr[1] = null;
	    //System.out.println(expr.length);

	}
	else {
		System.out.println("Please use OR or AND"); 
	}
}
  
  
  private void Query2c_CondExpr(CondExpr[] expr, int output_rel1, int output_rel2, int cond1_rel1, 
		  int cond1_rel2, int op1, int cond2_rel1, int cond2_rel2, int op2, String conjonction) {

	String and = new String("AND"); 
	String or = new String("OR"); 

	if (conjonction.equalsIgnoreCase(and)) {
	    expr[0].next  = null;
	    expr[0].op    = new AttrOperator(op1);
	    expr[0].type1 = new AttrType(AttrType.attrSymbol);
	    expr[0].type2 = new AttrType(AttrType.attrSymbol);
	    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),cond1_rel1);
	    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),cond1_rel2);
	
	    expr[1].op    = new AttrOperator(op2);
	    expr[1].next  = null;
	    expr[1].type1 = new AttrType(AttrType.attrSymbol);
	    expr[1].type2 = new AttrType(AttrType.attrSymbol);
	    /// ATTENTION 
	    expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer), cond2_rel1);
	    expr[1].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),cond2_rel2);
	 
	    expr[2] = null;
	}
	else if (conjonction.equals(or)) {	
	    expr[0].next = null;
	    expr[0].op   = new AttrOperator(op1);
	    expr[0].type1 = new AttrType(AttrType.attrSymbol);
	    expr[0].type2 = new AttrType(AttrType.attrSymbol);
	    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),cond1_rel1);
	    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),cond1_rel2);

	    expr[0].next = new CondExpr();
	    expr[0].next.op   = new AttrOperator(op2);
	    expr[0].next.type1 = new AttrType(AttrType.attrSymbol);
	    expr[0].next.type2 = new AttrType(AttrType.attrSymbol);
	    expr[0].next.operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),cond2_rel1);
	    expr[0].next.operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),cond2_rel2);
	    expr[0].next.next = null;

	    expr[1] = null;
	    System.out.println(expr.length);

	}
	else {
		System.out.println("Please use OR or AND"); 
	}
}


  public void Query1a(int output_rel1,
		  int output_rel2,
		  int cond_rel1,
		  int cond_rel2,
		  int op) {
    
    System.out.print("**********************Query1a starting *********************\n");
    boolean status = OK;
    
    // Build Index first
    IndexType b_index = new IndexType (IndexType.B_Index);

   
    //ExtendedSystemDefs.MINIBASE_CATALOGPTR.addIndex("sailors.in", "sid", b_index, 1);
    // }
    //catch (Exception e) {
    // e.printStackTrace();
    // System.err.print ("Failure to add index.\n");
      //  Runtime.getRuntime().exit(1);
    // }

    CondExpr [] outFilter  = new CondExpr[2];
    outFilter[0] = new CondExpr();
    outFilter[1] = new CondExpr();

    Query1a_CondExpr(outFilter, output_rel1, output_rel2, cond_rel1,cond_rel2,op);
    Tuple t = new Tuple();
    t = null;

    // Table1 table
    AttrType [] Rtypes = {    
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrInteger), 
    };
 
    short  []  Rsizes = new short[1] ;
    Rsizes[0] = 30;

    
    // Table2 table
    AttrType [] Stypes = {   
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrInteger)
    };
 
    short []   Ssizes = new short[1];
    Ssizes[0] = 30;
 

    // Final table
    AttrType [] Jtypes = {   
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrInteger), 
    };
 
    short []   Jsizes = new short[1];
    Jsizes[0] = 30;

    
    FldSpec [] Rprojection = {
    	       new FldSpec(new RelSpec(RelSpec.outer), 1),
    	       new FldSpec(new RelSpec(RelSpec.outer), 2),
    	       new FldSpec(new RelSpec(RelSpec.outer), 3),
    	       new FldSpec(new RelSpec(RelSpec.outer), 4),
    	    };

    
    FldSpec [] projCol1 = {
 	       new FldSpec(new RelSpec(RelSpec.outer), output_rel1),
 	       new FldSpec(new RelSpec(RelSpec.innerRel), output_rel2),
 	    };
 
    CondExpr [] selects = new CondExpr[1];
    selects[0] = null;
    
    
    //IndexType b_index = new IndexType(IndexType.B_Index);
    iterator.Iterator am = null;
   

    //_______________________________________________________________
    //*******************create an scan on the heapfile**************
    //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // create a tuple of appropriate size
        Tuple tt = new Tuple();
    try {
      tt.setHdr((short) 4, Rtypes, Rsizes);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }

    int sizett = tt.size();
    tt = new Tuple(sizett);
    try {
      tt.setHdr((short) 4, Rtypes, Rsizes);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    Heapfile        f = null;
    try {
      f = new Heapfile("table1.in");
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    
    Scan scan = null;
    
    try {
      scan = new Scan(f);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }

    // create the index file
    BTreeFile btf = null;
    try {
      btf = new BTreeFile("BTreeIndex", AttrType.attrInteger, 4, 1); 
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }
    
    RID rid = new RID();
    int key =0;
    Tuple temp = null;
    
    try {
      temp = scan.getNext(rid);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    while ( temp != null) {
      tt.tupleCopy(temp);
      
      try {
	key = tt.getIntFld(1);
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }
      
      try {
	btf.insert(new IntegerKey(key), rid); 
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }

      try {
	temp = scan.getNext(rid);
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }
    }
    
    // close the file scan
    scan.closescan();
    
    
    //_______________________________________________________________
    //*******************close an scan on the heapfile**************
    //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    System.out.print ("After Building btree index on Table1.3.\n\n");
    try {
      am = new IndexScan ( b_index, "table1.in",
			   "BTreeIndex", Rtypes, Rsizes, 4, 4,
			   Rprojection, null, 4, false);
    }
    
    catch (Exception e) {
      System.err.println ("*** Error creating scan for Index scan");
      System.err.println (""+e);
      Runtime.getRuntime().exit(1);
    }
   
    
    NestedLoopsJoins nlj = null;
    try {
      nlj = new NestedLoopsJoins (Rtypes, 4, Rsizes,
				  Stypes, 4, Ssizes,
				  10,
				  am, "table2.in",
				  outFilter, null, projCol1, 2);
    }
    catch (Exception e) {
      System.err.println ("*** Error preparing for nested_loop_join");
      System.err.println (""+e);
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }

    t = null;
    int compt = 0;
    try {
      while ((t = nlj.get_next()) != null) {
        t.print(Jtypes);
        compt = compt +1;
      }
      //System.out.println(compt);
    }
    catch (Exception e) {
      System.err.println (""+e);
      e.printStackTrace();
       Runtime.getRuntime().exit(1);
    }


    System.out.println ("\n"); 
    try {
    	nlj.close();
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    
    if (status != OK) {
      //bail out
   
      Runtime.getRuntime().exit(1);}
      }

  
  
    public void Query1b(int output_rel1, int output_rel2, int cond1_rel1, 
  		  int cond1_rel2, int op1, int cond2_rel1, int cond2_rel2, int op2, String conjonction) {
        
        System.out.print("**********************Query1b starting *********************\n");
        boolean status = OK;
        
        // Table2, Table1, Q Queries.
        /*
        System.out.print ("Query: \n"
    		      + "  SELECT Table1.col1, Table2.col1\n"
    		      + "  FROM   Table2, Table1\n"
    		      + "  WHERE  Table1.col3 < Table2.col3"
    		      + "         AND "
    		      + "         Table1.col4 <= Table2.col4\n\n");
        */
        
        
        // Build Index first
        IndexType bb_index = new IndexType (IndexType.B_Index);

    	String and = new String("AND"); 
    	String or = new String("OR"); 
        CondExpr [] outFilter_and  = new CondExpr[3];
        CondExpr [] outFilter_or  = new CondExpr[2];

       
        //ExtendedSystemDefs.MINIBASE_CATALOGPTR.addIndex("sailors.in", "sid", b_index, 1);
        // }
        //catch (Exception e) {
        // e.printStackTrace();
        // System.err.print ("Failure to add index.\n");
          //  Runtime.getRuntime().exit(1);
        // }

        
    	if (conjonction.equalsIgnoreCase(and)) {
            outFilter_and[0] = new CondExpr();
            outFilter_and[1] = new CondExpr();
            outFilter_and[2] = new CondExpr();
            Query1b_CondExpr(outFilter_and, output_rel1, output_rel2, cond1_rel1, cond1_rel2, op1, cond2_rel1, cond2_rel2, op2, conjonction);
    	}
    	else if (conjonction.equals(or)) {	
            outFilter_or[0] = new CondExpr();
            outFilter_or[1] = new CondExpr();
            Query1b_CondExpr(outFilter_or, output_rel1, output_rel2, cond1_rel1, cond1_rel2, op1, cond2_rel1, cond2_rel2, op2, conjonction);
    	}


        Tuple t = new Tuple();
        t = null;

        // Table1 table
        AttrType [] Rtypesb = {    
          new AttrType(AttrType.attrInteger), 
          new AttrType(AttrType.attrInteger), 
          new AttrType(AttrType.attrInteger),
          new AttrType(AttrType.attrInteger), 
        };

        short  []  Rsizesb = new short[1] ;
        Rsizesb[0] = 30;

        
        // Table2 table
        AttrType [] Stypesb = {   
          new AttrType(AttrType.attrInteger), 
          new AttrType(AttrType.attrInteger), 
          new AttrType(AttrType.attrInteger), 
          new AttrType(AttrType.attrInteger)
        };
     
        short []   Ssizesb = new short[1];
        Ssizesb[0] = 30;
     

        // Final table
        AttrType [] Jtypesb = {   
          new AttrType(AttrType.attrInteger), 
          new AttrType(AttrType.attrInteger), 
        };
     
        short []   Jsizesb = new short[1];
        Jsizesb[0] = 30;
        
        FldSpec [] Rprojectionb = {
        	       new FldSpec(new RelSpec(RelSpec.outer), 1),
        	       new FldSpec(new RelSpec(RelSpec.outer), 2),
        	       new FldSpec(new RelSpec(RelSpec.outer), 3),
        	       new FldSpec(new RelSpec(RelSpec.outer), 4),
        	    };

        
        FldSpec [] projFinalb = {
     	       new FldSpec(new RelSpec(RelSpec.outer), output_rel1),
     	       new FldSpec(new RelSpec(RelSpec.innerRel), output_rel2),
     	    };
        
     
        CondExpr [] selects = new CondExpr[1];
        selects[0] = null;
        
        
        //IndexType b_index = new IndexType(IndexType.B_Index);
        iterator.Iterator amb = null;
       

        //_______________________________________________________________
        //*******************create an scan on the heapfile**************
        //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        // create a tuple of appropriate size
            Tuple ttb = new Tuple();
        try {
          ttb.setHdr((short) 4, Rtypesb, Rsizesb);
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
        }

        int sizett = ttb.size();
        ttb = new Tuple(sizett);
        try {
          ttb.setHdr((short) 4, Rtypesb, Rsizesb);
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
        }
        Heapfile        f = null;
        try {
          f = new Heapfile("table1.in");
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
        }
        
        Scan scan = null;
        
        try {
          scan = new Scan(f);
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
          Runtime.getRuntime().exit(1);
        }

        // create the index file
        BTreeFile btf = null;
        try {
          btf = new BTreeFile("BTreeIndex", AttrType.attrInteger, 4, 1); 
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
          Runtime.getRuntime().exit(1);
        }
        
        RID rid = new RID();
        int key =0;
        Tuple temp = null;
        
        try {
          temp = scan.getNext(rid);
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
        }
        while ( temp != null) {
          ttb.tupleCopy(temp);
          
          try {
    	key = ttb.getIntFld(1);
          }
          catch (Exception e) {
    	status = FAIL;
    	e.printStackTrace();
          }
          
          try {
    	btf.insert(new IntegerKey(key), rid); 
          }
          catch (Exception e) {
    	status = FAIL;
    	e.printStackTrace();
          }

          try {
    	temp = scan.getNext(rid);
          }
          catch (Exception e) {
    	status = FAIL;
    	e.printStackTrace();
          }
        }
        
        // close the file scan
        scan.closescan();
        
        
        //_______________________________________________________________
        //*******************close an scan on the heapfile**************
        //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

        System.out.print ("After Building btree index on Table1.3.\n\n");
        try {
          amb = new IndexScan ( bb_index, "table1.in",
    			   "BTreeIndex", Rtypesb, Rsizesb, 4, 4,
    			   Rprojectionb, null, 1, false);
        }
        
        catch (Exception e) {
          System.err.println ("*** Error creating scan for Index scan");
          System.err.println (""+e);
          Runtime.getRuntime().exit(1);
        }
       
        
        NestedLoopsJoins nljb = null;
        try {
        	if (conjonction.equalsIgnoreCase(and)) {
                nljb = new NestedLoopsJoins (Rtypesb, 4, Rsizesb,
      				  Stypesb, 4, Ssizesb,
      				  10,
      				  amb, "table2.in",
      				  outFilter_and, null, projFinalb, 2);
        	}
        	else if (conjonction.equals(or)) {	
                nljb = new NestedLoopsJoins (Rtypesb, 4, Rsizesb,
      				  Stypesb, 4, Ssizesb,
      				  10,
      				  amb, "table2.in",
      				  outFilter_or, null, projFinalb, 2);
        	}
        }
        catch (Exception e) {
          System.err.println ("*** Error preparing for nested_loop_join");
          System.err.println (""+e);
          e.printStackTrace();
          Runtime.getRuntime().exit(1);
        }
        t = null;
        int co = 0;
        try {
          while ((t = nljb.get_next()) != null) {
            t.print(Jtypesb);
            co = co +1;
          }
          //System.out.println("number of outputs = " + co);
        }
        catch (Exception e) {
            System.err.println (""+e);
            e.printStackTrace();
             Runtime.getRuntime().exit(1);
          }


          System.out.println ("\n"); 
          try {
          	nljb.close();
          }
          catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
          }
          
       
          if (status != OK) {
            //bail out
         
            Runtime.getRuntime().exit(1);}
    }
    
    
    public void Query2a(int output_rel1,
  		  int output_rel2,
  		  int cond_rel1,
  		  int cond_rel2,
  		  int op) {
      
      System.out.print("**********************Query2a strating *********************\n");
      boolean status = OK;
      
      // Build Index first
      IndexType b_index = new IndexType (IndexType.B_Index);

     
      //ExtendedSystemDefs.MINIBASE_CATALOGPTR.addIndex("sailors.in", "sid", b_index, 1);
      // }
      //catch (Exception e) {
      // e.printStackTrace();
      // System.err.print ("Failure to add index.\n");
        //  Runtime.getRuntime().exit(1);
      // }

      CondExpr [] outFilter  = new CondExpr[2];
      outFilter[0] = new CondExpr();
      outFilter[1] = new CondExpr();

      Query1a_CondExpr(outFilter, output_rel1, output_rel2, cond_rel1,cond_rel2,op);
      Tuple t = new Tuple();
      t = null;

      // Q table
      AttrType [] Qtypes = {    
        new AttrType(AttrType.attrInteger), 
        new AttrType(AttrType.attrInteger), 
        new AttrType(AttrType.attrInteger),
        new AttrType(AttrType.attrInteger), 
      };
   
      short  []  Qsizes = new short[1] ;
      Qsizes[0] = 30;   

      // Final table
      AttrType [] Jtypes = {   
        new AttrType(AttrType.attrInteger), 
        new AttrType(AttrType.attrInteger), 
      };
   
      short []   Jsizes = new short[1];
      Jsizes[0] = 30;

      
      FldSpec [] Qprojection = {
      	       new FldSpec(new RelSpec(RelSpec.outer), 1),
      	       new FldSpec(new RelSpec(RelSpec.outer), 2),
      	       new FldSpec(new RelSpec(RelSpec.outer), 3),
      	       new FldSpec(new RelSpec(RelSpec.outer), 4),
      	    };

      
      FldSpec [] projFinal = {
   	       new FldSpec(new RelSpec(RelSpec.outer), output_rel1),
   	       new FldSpec(new RelSpec(RelSpec.innerRel), output_rel2),
   	    };
   
      CondExpr [] selects = new CondExpr[1];
      selects[0] = null;
      
      
      //IndexType b_index = new IndexType(IndexType.B_Index);
      iterator.Iterator am = null;
     

      //_______________________________________________________________
      //*******************create an scan on the heapfile**************
      //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
      // create a tuple of appropriate size
          Tuple tt = new Tuple();
      try {
        tt.setHdr((short) 4, Qtypes, Qsizes);
      }
      catch (Exception e) {
        status = FAIL;
        e.printStackTrace();
      }

      int sizett = tt.size();
      tt = new Tuple(sizett);
      try {
        tt.setHdr((short) 4, Qtypes, Qsizes);
      }
      catch (Exception e) {
        status = FAIL;
        e.printStackTrace();
      }
      Heapfile        f = null;
      try {
        f = new Heapfile("table1.in");
      }
      catch (Exception e) {
        status = FAIL;
        e.printStackTrace();
      }
      
      Scan scan = null;
      
      try {
        scan = new Scan(f);
      }
      catch (Exception e) {
        status = FAIL;
        e.printStackTrace();
        Runtime.getRuntime().exit(1);
      }

      // create the index file
      BTreeFile btf = null;
      try {
        btf = new BTreeFile("BTreeIndex", AttrType.attrInteger, 4, 1); 
      }
      catch (Exception e) {
        status = FAIL;
        e.printStackTrace();
        Runtime.getRuntime().exit(1);
      }
      
      RID rid = new RID();
      int key =0;
      Tuple temp = null;
      
      try {
        temp = scan.getNext(rid);
      }
      catch (Exception e) {
        status = FAIL;
        e.printStackTrace();
      }
      while ( temp != null) {
        tt.tupleCopy(temp);
        
        try {
  	key = tt.getIntFld(1);
        }
        catch (Exception e) {
  	status = FAIL;
  	e.printStackTrace();
        }
        
        try {
  	btf.insert(new IntegerKey(key), rid); 
        }
        catch (Exception e) {
  	status = FAIL;
  	e.printStackTrace();
        }

        try {
  	temp = scan.getNext(rid);
        }
        catch (Exception e) {
  	status = FAIL;
  	e.printStackTrace();
        }
      }
      
      // close the file scan
      scan.closescan();
      
      
      //_______________________________________________________________
      //*******************close an scan on the heapfile**************
      //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

      System.out.print ("After Building btree index on Table1.3.\n\n");
      try {
        am = new IndexScan ( b_index, "table1.in",
  			   "BTreeIndex", Qtypes, Qsizes, 4, 4,
  			   Qprojection, null, 4, false);
      }
      
      catch (Exception e) {
        System.err.println ("*** Error creating scan for Index scan");
        System.err.println (""+e);
        Runtime.getRuntime().exit(1);
      }
     
      
      SelfJoin1 sj = null;
      try {
    	  sj = new SelfJoin1 (Qtypes, 4, Qsizes,
  				  10,
  				  am,
  				  outFilter, null, projFinal, 2);
      }
      catch (Exception e) {
        System.err.println ("*** Error preparing for nested_loop_join");
        System.err.println (""+e);
        e.printStackTrace();
        Runtime.getRuntime().exit(1);
      }

      t = null;
      int com =0;
      try {
        while ((t = sj.get_next()) != null) {
          t.print(Jtypes);
          com = com +1;
          //qcheck3.Check(t);
        }
        //System.out.println(com);
      }
      catch (Exception e) {
        System.err.println (""+e);
        e.printStackTrace();
         Runtime.getRuntime().exit(1);
      }


      System.out.println ("\n"); 
      try {
    	  sj.close();
      }
      catch (Exception e) {
        status = FAIL;
        e.printStackTrace();
      }
      
      if (status != OK) {
        //bail out
     
        Runtime.getRuntime().exit(1);}
        }


    public void Query2b(int output_rel1, int output_rel2, int cond1_rel1, 
    		  int cond1_rel2, int op1, int cond2_rel1, int cond2_rel2, int op2, String conjonction) {
          
          System.out.print("**********************Query2b starting*********************\n\n");
          boolean status = OK;
                    
          // Build Index first
          IndexType b_index = new IndexType (IndexType.B_Index);

       	String and = new String("AND"); 
      	String or = new String("OR"); 
          CondExpr [] outFilter_and  = new CondExpr[3];
          CondExpr [] outFilter_or  = new CondExpr[2];

      	if (conjonction.equalsIgnoreCase(and)) {
              outFilter_and[0] = new CondExpr();
              outFilter_and[1] = new CondExpr();
              outFilter_and[2] = new CondExpr();
              Query1b_CondExpr(outFilter_and, output_rel1, output_rel2, cond1_rel1, cond1_rel2, op1, cond2_rel1, cond2_rel2, op2, conjonction);
      	}
      	else if (conjonction.equals(or)) {	
              outFilter_or[0] = new CondExpr();
              outFilter_or[1] = new CondExpr();
              System.out.println("*************************************************************************************************************");
              System.out.println("********The paper does not cope with 'OR'. Here is the result of your query with an 'AND' operator***********");
              System.out.println("*************************************************************************************************************");
              Query1b_CondExpr(outFilter_or, output_rel1, output_rel2, cond1_rel1, cond1_rel2, op1, cond2_rel1, cond2_rel2, op2, conjonction);
      	}


          Tuple t = new Tuple();
          t = null;

          // Q table
          AttrType [] Qtypes = {    
            new AttrType(AttrType.attrInteger), 
            new AttrType(AttrType.attrInteger), 
            new AttrType(AttrType.attrInteger),
            new AttrType(AttrType.attrInteger), 
          };
       
          short  []  Qsizes = new short[1] ;
          Qsizes[0] = 30;   

          // Final table
          AttrType [] Jtypes = {   
            new AttrType(AttrType.attrInteger), 
            new AttrType(AttrType.attrInteger), 
          };
       
          short []   Jsizes = new short[1];
          Jsizes[0] = 30;

          
          FldSpec [] Qprojection = {
          	       new FldSpec(new RelSpec(RelSpec.outer), 1),
          	       new FldSpec(new RelSpec(RelSpec.outer), 2),
          	       new FldSpec(new RelSpec(RelSpec.outer), 3),
          	       new FldSpec(new RelSpec(RelSpec.outer), 4),
          	    };

          
          FldSpec [] projFinal = {
       	       new FldSpec(new RelSpec(RelSpec.outer), output_rel1),
       	       new FldSpec(new RelSpec(RelSpec.innerRel), output_rel2),
       	    };
       
          CondExpr [] selects = new CondExpr[1];
          selects[0] = null;
          
          
          //IndexType b_index = new IndexType(IndexType.B_Index);
          iterator.Iterator am = null;
          


          //_______________________________________________________________
          //*******************create an scan on the heapfile**************
          //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
          // create a tuple of appropriate size
              Tuple tt = new Tuple();
          try {
            tt.setHdr((short) 4, Qtypes, Qsizes);
          }
          catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
          }

          int sizett = tt.size();
          tt = new Tuple(sizett);
          try {
            tt.setHdr((short) 4, Qtypes, Qsizes);
          }
          catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
          }
          Heapfile        f = null;
          try {
            f = new Heapfile("table1.in");
          }
          catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
          }
          
          Scan scan = null;
          
          try {
            scan = new Scan(f);
          }
          catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
          }

          // create the index file
          BTreeFile btf = null;
          try {
            btf = new BTreeFile("BTreeIndex", AttrType.attrInteger, 4, 1); 
          }
          catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
          }
          
          RID rid = new RID();
          int key =0;
          Tuple temp = null;
          
          try {
            temp = scan.getNext(rid);
          }
          catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
          }
          while ( temp != null) {
            tt.tupleCopy(temp);
            
            try {
      	key = tt.getIntFld(1);
            }
            catch (Exception e) {
      	status = FAIL;
      	e.printStackTrace();
            }
            
            try {
      	btf.insert(new IntegerKey(key), rid); 
            }
            catch (Exception e) {
      	status = FAIL;
      	e.printStackTrace();
            }

            try {
      	temp = scan.getNext(rid);
            }
            catch (Exception e) {
      	status = FAIL;
      	e.printStackTrace();
            }
          }
          
          // close the file scan
          scan.closescan();
          
          
          //_______________________________________________________________
          //*******************close an scan on the heapfile**************
          //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

          System.out.print ("After Building btree index on Table1.3.\n\n");
          try {
            am = new IndexScan ( b_index, "table1.in",
      			   "BTreeIndex", Qtypes, Qsizes, 4, 4,
      			   Qprojection, null, 4, false);
          }
          
          catch (Exception e) {
            System.err.println ("*** Error creating scan for Index scan");
            System.err.println (""+e);
            Runtime.getRuntime().exit(1);
          }
         
          SelfJoin2 slj2 = null;
          try {
        	  //System.out.println("AND");
          	if (conjonction.equalsIgnoreCase(and)) {
                  slj2 = new SelfJoin2 (Qtypes, 4, Qsizes,
        				  10,
        				  am,
        				  outFilter_and, null, projFinal, 2);
          	}
          	else if (conjonction.equals(or)) {	
          	  //System.out.println("OR");
                  slj2 = new SelfJoin2 (Qtypes, 4, Qsizes,
        				  10,
        				  am,
        				  outFilter_or, null, projFinal, 2);
          	}
          }
          catch (Exception e) {
            System.err.println ("*** Error preparing for self_join_2");
            System.err.println (""+e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
          }
          t = null;
          int c = 0;
          try {
            while ((t = slj2.get_next()) != null) {
              t.print(Qtypes);
              c = c+1;
            }
            //System.out.println("number of outputs = " + c);

          }
          catch (Exception e) {
              System.err.println (""+e);
              e.printStackTrace();
               Runtime.getRuntime().exit(1);
            }


            System.out.println ("\n"); 
            try {
            	slj2.close();
            }
            catch (Exception e) {
              status = FAIL;
              e.printStackTrace();
            }
            
         
            if (status != OK) {
              //bail out
           
              Runtime.getRuntime().exit(1);}
      }
  

    public void Query2c(int output_rel1, int output_rel2, int cond1_rel1, 
  		  int cond1_rel2, int op1, int cond2_rel1, int cond2_rel2, int op2, String conjonction) {
        
        System.out.print("**********************Query2c starting *********************\n");
        boolean status = OK;
        

     	String and = new String("AND"); 
    	String or = new String("OR"); 
        CondExpr [] outFilter_and  = new CondExpr[3];
        CondExpr [] outFilter_or  = new CondExpr[2];

        
    	if (conjonction.equalsIgnoreCase(and)) {
            outFilter_and[0] = new CondExpr();
            outFilter_and[1] = new CondExpr();
            outFilter_and[2] = new CondExpr();
            Query2c_CondExpr(outFilter_and, output_rel1, output_rel2, cond1_rel1, cond1_rel2, op1, cond2_rel1, cond2_rel2, op2, conjonction);
    	}
    	else if (conjonction.equals(or)) {	
            outFilter_or[0] = new CondExpr();
            outFilter_or[1] = new CondExpr();
            Query2c_CondExpr(outFilter_or, output_rel1, output_rel2, cond1_rel1, cond1_rel2, op1, cond2_rel1, cond2_rel2, op2, conjonction);
            System.out.println("*************************************************************************************************************");
            System.out.println("********The paper does not cope with 'OR'. Here is the result of your query with an 'AND' operator***********");
            System.out.println("*************************************************************************************************************");
    	}

        Tuple t = new Tuple();
        t = null;

        // Outer table
        AttrType [] OuterTypes = {    
          new AttrType(AttrType.attrInteger), 
          new AttrType(AttrType.attrInteger), 
          new AttrType(AttrType.attrInteger),
          new AttrType(AttrType.attrInteger), 
        };
     
        short  []  OuterSizes = new short[1] ;
        OuterSizes[0] = 30;   

        // Inner table
        AttrType [] InnerTypes = {    
          new AttrType(AttrType.attrInteger), 
          new AttrType(AttrType.attrInteger), 
          new AttrType(AttrType.attrInteger),
          new AttrType(AttrType.attrInteger), 
        };
     
        short  []  InnerSizes = new short[1] ;
        InnerSizes[0] = 30;   

        // Final table
        AttrType [] Jtypes = {   
          new AttrType(AttrType.attrInteger), 
          new AttrType(AttrType.attrInteger), 
        };
     
        short []   Jsizes = new short[1];
        Jsizes[0] = 30;

        
        FldSpec [] OuterProjection = {
        	       new FldSpec(new RelSpec(RelSpec.outer), 1),
        	       new FldSpec(new RelSpec(RelSpec.outer), 2),
        	       new FldSpec(new RelSpec(RelSpec.outer), 3),
        	       new FldSpec(new RelSpec(RelSpec.outer), 4),
        	    };
        
        FldSpec [] InnerProjection = {
     	       new FldSpec(new RelSpec(RelSpec.innerRel), 1),
     	       new FldSpec(new RelSpec(RelSpec.innerRel), 2),
     	       new FldSpec(new RelSpec(RelSpec.innerRel), 3),
     	       new FldSpec(new RelSpec(RelSpec.innerRel), 4),
     	    };

        
        FldSpec [] projFinal = {
     	       new FldSpec(new RelSpec(RelSpec.outer), output_rel1),
     	       new FldSpec(new RelSpec(RelSpec.innerRel), output_rel2),
     	    };
     
        CondExpr [] selects = new CondExpr[1];
        selects[0] = null;
        
        
        //IndexType b_index = new IndexType(IndexType.B_Index);
        iterator.Iterator amOuter = null;
        iterator.Iterator amInner = null;

        try {
        	amOuter = new FileScan("table1.in", OuterTypes, OuterSizes, 
      				  (short)4, (short)4,
      				  OuterProjection, null);    		
          }
          catch (Exception e) {
            status = FAIL;
            System.err.println (""+e);
          }
          
          if (status != OK) {
            //bail out
            System.err.println ("*** Error setting up scan for reserves");
            Runtime.getRuntime().exit(1);
          }


          try {
        	  amInner = new FileScan("table2.in", InnerTypes, InnerSizes, 
      				  (short)4, (short)4,
      				OuterProjection, null);

            }
            catch (Exception e) {
              status = FAIL;
              System.err.println (""+e);
            }
            
            if (status != OK) {
              //bail out
              System.err.println ("*** Error setting up scan for reserves");
              Runtime.getRuntime().exit(1);
            }

        IEJoin iejoin = null;
        try {
      	  System.out.println("AND");
        	if (conjonction.equalsIgnoreCase(and)) {
        		iejoin = new IEJoin (OuterTypes, 4, OuterSizes,
        			  InnerTypes, 4, InnerSizes,
      				  10,
      				  amOuter,
      				  amInner,
      				  outFilter_and, null, projFinal, 2);
        	}
        	else if (conjonction.equals(or)) {	
        	  System.out.println("OR");
      		iejoin = new IEJoin (OuterTypes, 4, OuterSizes,
      			  InnerTypes, 4, InnerSizes,
    				  10,
    				  amOuter,
    				  amInner,
    				  outFilter_or, null, projFinal, 2);
        	}
        }
        catch (Exception e) {
          System.err.println ("*** Error preparing for ie_join ***");
          System.err.println (""+e);
          e.printStackTrace();
          Runtime.getRuntime().exit(1);
        }
        t = null;
        int compteu = 0;
        try {
          while ((t = iejoin.get_next()) != null) {
            t.print(Jtypes);
            compteu = compteu + 1 ;
          }
          //System.out.println("number of outputs = " + compteu);

        }
        catch (Exception e) {
            System.err.println (""+e);
            e.printStackTrace();
             Runtime.getRuntime().exit(1);
          }


          System.out.println ("\n"); 
          try {
        	  iejoin.close();
          }
          catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
          }
          
       
          if (status != OK) {
            //bail out
         
            Runtime.getRuntime().exit(1);}
    }

  private void Disclaimer() {
	    System.out.print ("\n\nAny resemblance of persons in this database to"
	         + " people living or dead\nis purely coincidental. The contents of "
	         + "this database do not reflect\nthe views of the University,"
	         + " the Computer  Sciences Department or the\n"
	         + "developers...\n\n");
	  }

}

public class JoinTest2
{
  public static void main(String argv[]) throws IOException
  {
    boolean sortstatus;
    //SystemDefs global = new SystemDefs("bingjiedb", 100, 70, null);
    //JavabaseDB.openDB("/tmp/nwangdb", 5000);
        
    try(Scanner scanner = new Scanner(System.in)){
    	System.out.print("Number of tables you need : ");
    	Integer nb_table = scanner.nextInt(); 
    	System.out.println("You chose to use " + nb_table + " tables");
    	if (nb_table == 1) {
    		
    	    try(Scanner scanner2 = new Scanner(System.in)){
        		System.out.println("Please enter the path to the table : ");
        		String pathTable = scanner2.nextLine(); 
        	    JoinsDriver2 jjoin = new JoinsDriver2(pathTable);
        	    
	    	    try(Scanner scanner3 = new Scanner(System.in)){
	        		System.out.println("Please enter the path to the query : ");
	        		String pathQuery = scanner3.nextLine(); 
	        	    sortstatus = jjoin.runTests2(pathQuery, 0);
	        	    if (sortstatus != true) {
	        	      System.out.println("Error ocurred during join tests");
	        	    }
	        	    else {
	        	      System.out.println("join tests completed successfully");
	        	    }
    	    }
    	    }
    	}
    	if (nb_table == 2) {
    		
    	    try(Scanner scanner2 = new Scanner(System.in)){
        		System.out.println("Please enter the path to the first table (outer relation) : ");
        		String pathTable1 = scanner2.nextLine(); 
        		try(Scanner scanner3 = new Scanner(System.in)){
            		System.out.println("Please enter the path to the second table (inner relation) : ");
            		String pathTable2 = scanner3.nextLine(); 
            	    JoinsDriver2 jjoin = new JoinsDriver2(pathTable1, pathTable2);
            	    try(Scanner scanner4 = new Scanner(System.in)){
                		System.out.println("Please enter the path to the query : ");
                		String pathQuery = scanner4.nextLine(); 
                		
                	   	  int lines = 0; 
                		    try{
                		    	InputStream flux = new FileInputStream(pathQuery); 
                		    	InputStreamReader text = new InputStreamReader(flux);
                		    	BufferedReader buff = new BufferedReader(text);
                		    	while (buff.readLine() != null) lines++;
                	    	buff.close(); 
                	    	}		
                	    	catch (Exception e){
                	    	System.out.println(e.toString());
                	    	}
                		    
                		    if (lines == 5) {
                	    	    try(Scanner scanner5 = new Scanner(System.in)){
                	        		System.out.println("You have two options for this query : Nested Loop Join (enter 1) or IE join (enter 2) : ");
                	        		Integer query = scanner5.nextInt(); 
                	        	    sortstatus = jjoin.runTests2(pathQuery, query);
                	        	    if (sortstatus != true) {
                          	      System.out.println("Error ocurred during join tests");
                	        	    }
                	        	    else {
                              	      System.out.println("join tests completed successfully");
                              	    }
                	    	    }
                		    }
                	       else{
                	    	   sortstatus = jjoin.runTests2(pathQuery, 0);
           	        	    	if (sortstatus != true) {
           	        	    		System.out.println("Error ocurred during join tests");
           	        	    	}
           	        	    	else {
                          	      System.out.println("join tests completed successfully");
                          	    }
       	        	    }

            	    }
        		}
        	    
    	    }
    	    
    	 }
    }
    
    /*
    String path_table = "../../../QueriesData_newvalues_modified/table_test.txt";
    String path_query = "../../../QueriesData_newvalues/query_2a.txt";
    int compt = 0;
    int r1 = 1;
    int r2 = 2;
    int r3 = 3;
    ArrayList<Long> time_list = new ArrayList<Long>();
    ArrayList<Integer> nb_tuples_list = new ArrayList<Integer>();
    
	//BufferedWriter bw = new BufferedWriter( new FileWriter(path));
    BufferedWriter bw = null;
    FileWriter fw =null;
    int nb_tuples = 0;
    
    try {
    	String content = "attrInteger,attrInteger,attrInteger,attrInteger";
    	fw = new FileWriter(path_table);
    	bw = new BufferedWriter(fw);
    	bw.write(content);

    	while(true) {
    		String content12 = "\n" + Integer.toString(compt) + "," + Integer.toString(r1) + "," + Integer.toString(r2) + "," + Integer.toString(r3);
    		bw.write(content12);
    		nb_tuples = nb_tuples + 1;
    		
    		
    		if (nb_tuples % 100 == 0) {           
    				  try {
    			      if (bw != null)
    			          bw.close();

    			      if (fw != null)
    			          fw.close();
    			  } catch (IOException ex) {
    			      System.err.format("IOException: %s%n", ex);
    			  }

        		System.out.println("Number of tuples : " + nb_tuples);
        		nb_tuples_list.add(nb_tuples);
        		long startTime = System.currentTimeMillis();
        		
        		try {
        			JoinsDriver2 jjoin = new JoinsDriver2(path_table);    
        			sortstatus = jjoin.runTests2(path_query, 0);
        			if (sortstatus != true) {
        				System.out.println("Error ocurred during join tests");
        			}
        			else {
        				System.out.println("join tests completed successfully");
        			}
        		}
        		catch(Exception ex){
        			System.out.println(nb_tuples_list);
        			System.out.println(time_list);
        		}
    			System.out.println(nb_tuples_list);
    			System.out.println(time_list);

        	    time_list.add(System.currentTimeMillis() - startTime);
        		System.out.println("Execution time for " + nb_tuples + " tuples is " + (System.currentTimeMillis() - startTime) + " milliseconds \n");
            	fw = new FileWriter(path_table, true);
            	bw = new BufferedWriter(fw);
    		}
    		
    		
        	compt = compt + 4;
        	r1 = r1 + 4;
        	r2 = r2 + 4;
        	r3 = r3 + 4;
   
    	}
    }catch(IOException e){System.err.format("IO Exception %s%n", e);}
    
  finally {            try {
      if (bw != null)
          bw.close();

      if (fw != null)
          fw.close();
  } catch (IOException ex) {
      System.err.format("IOException: %s%n", ex);
  }
  }
  
  */
  
   /*
    
    String path_table1 = "../../../../../QueriesData_newvalues_modified/R.txt";
    String path_table2 = "../../../../QueriesData_newvalues_modified/R.txt";
    String path_query = "../../../../QueriesData_newvalues_modified/query_1b.txt";
    
	JoinsDriver2 jjoin = new JoinsDriver2(path_table1, path_table2);
	sortstatus = jjoin.runTests2(path_query, 0);
	if (sortstatus != true) {
		System.out.println("Error ocurred during join tests");
	}
	else {
		System.out.println("join tests completed successfully");
	}

	*/
	
	/*
	JoinsDriver2 jjoin2 = new JoinsDriver2(path_table1);
	*/
   }
}
