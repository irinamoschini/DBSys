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


//Define the R schema
class R {
  public int col1;
  public int col2;
  public int col3;
  public int col4;
  ;
  public R (int _col1, int _col2, int _col3, int _col4) {
    col1 = _col1;
    col2 = _col2;
    col3 = _col3;
    col4 = _col4;
  }
}
 
//Define the S schema
class S {
  public int col1;
  public int col2;
  public int col3;
  public int col4;
  

  public S (int _col1, int _col2, int _col3, int _col4) {
    col1 = _col1;
    col2 = _col2;
    col3 = _col3;
    col4 = _col4;
  }
}
 
//Define the Q schema
class Q {
  public int col1;
  public int col2;
  public int col3;
  public int col4;
  

  public Q (int _col1, int _col2, int _col3, int _col4) {
    col1 = _col1;
    col2 = _col2;
    col3 = _col3;
    col4 = _col4;
  }
}


class JoinTest2 implements GlobalConst{
  
  private static final boolean OK = false;
private Vector r; 
  private Vector s; 
  private Vector q;

public JoinTest2() {
  try{
    File fileR = new File("../../../../QueriesData_newvalues/R.txt");
    Scanner scannerR = new Scan(fileR);
    scannerR.nextLine();
    while (scannerR.hasNextLine()){
       String line = scannerR.nextLine(); 
       String[] lineWords = line.split(",");
       int r1 = Integer.parseInt(lineWords[0])
       int r2 = Integer.parseInt(lineWords[1])
       int r3 = Integer.parseInt(lineWords[2])
       int r4 = Integer.parseInt(lineWords[3])
       r.addElement(new R(r1, r2, r3,r4));
    }
  } catch (FileNotFoundException e){
      System.out.println("File given not found.")
  }
}

  try{
    File fileS = new File("../../../../QueriesData_newvalues/S.txt");
    Scanner scannerS = new Scan(fileS);
    scannerS.nextLine();
    while (scannerS.hasNextLine()){
       String line = scannerS.nextLine(); 
       String[] lineWords = line.split(",");
       int s1 = Integer.parseInt(lineWords[0])
       int s2 = Integer.parseInt(lineWords[1])
       int s3 = Integer.parseInt(lineWords[2])
       int s4 = Integer.parseInt(lineWords[3])
       s.addElement(new S(s1, s2, s3,s4));
    }
  } catch (FileNotFoundException e){
      System.out.println("File given not found.")
  }
}

  try{
    File fileQ = new File("../../../../QueriesData_newvalues/Q.txt");
    Scanner scannerQ = new Scan(fileQ);
    scannerQ.nextLine();
    while (scannerQ.hasNextLine()){
       String line = scannerQ.nextLine(); 
       String[] lineWords = line.split(",");
       int q1 = Integer.parseInt(lineWords[0])
       int q2 = Integer.parseInt(lineWords[1])
       int q3 = Integer.parseInt(lineWords[2])
       int q4 = Integer.parseInt(lineWords[3])
       q.addElement(new Q(q1, q2, q3,q4));
    }
  } catch (FileNotFoundException e){
      System.out.println("File given not found.")
  }
}

    // Initial size setting to accomodate the entire dataset
    // Each relation admits 4 attributes 

    boolean status = OK;
    int nums = s.size();
    int nums_attrs = 4;
    int numr = r.size();
    int numr_attrs = 4;
    int numq = q.size();
    int numq_attrs = 4;


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
    
    // creating the S relation
    AttrType [] Stypes = new AttrType[4];
    Stypes[0] = new AttrType (AttrType.attrInteger);
    Stypes[1] = new AttrType (AttrType.attrString);
    Stypes[2] = new AttrType (AttrType.attrInteger);
    Stypes[3] = new AttrType (AttrType.attrReal);

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
    
    // inserting the tuple into file "S"
    RID             rid;
    Heapfile        f = null;
    try {
      f = new Heapfile("s.in");
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
	t.setIntFld(1, ((S)s.elementAt(i)).col1);
	t.setStrFld(2, ((S)s.elementAt(i)).col2);
	t.setIntFld(3, ((S)s.elementAt(i)).col3);
	t.setFloFld(4, ((S)s.elementAt(i)).col4);
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
      System.err.println ("*** Error creating relation for S");
      Runtime.getRuntime().exit(1);
    }
    
    //creating the R relation
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
    
    // inserting the tuple into file r
    //RID             rid;
    f = null;
    try {
      f = new Heapfile("r.in");
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
	t.setIntFld(1, ((R)boats.elementAt(i)).col1);
	t.setStrFld(2, ((R)boats.elementAt(i)).col2);
	t.setStrFld(3, ((R)boats.elementAt(i)).col3);
	t.setStrFld(4, ((R)boats.elementAt(i)).col4);
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
      System.err.println ("*** Error creating relation for R");
      Runtime.getRuntime().exit(1);
    }
    
    //creating Q relation
    AttrType [] Qtypes = new AttrType[4];
    Qtypes[0] = new AttrType (AttrType.attrInteger);
    Qtypes[1] = new AttrType (AttrType.attrInteger);
    Qtypes[2] = new AttrType (AttrType.attrString);
    Qtypes[3] = new AttrType (AttrType.attrInteger);

    short [] Qsizes = new short [1];
    Qsizes[0] = 0; 
    t = new Tuple();
    try {
      t.setHdr((short) 4,Qtypes, Qsizes);
    }
    catch (Exception e) {
      System.err.println("*** error in Tuple.setHdr() ***");
      status = FAIL;
      e.printStackTrace();
    }
    
    size = t.size();
    
    // inserting the tuple into file q
    //RID             rid;
    f = null;
    try {
      f = new Heapfile("q.in");
    }
    catch (Exception e) {
      System.err.println("*** error in Heapfile constructor ***");
      status = FAIL;
      e.printStackTrace();
    }
    
    t = new Tuple(size);
    try {
      t.setHdr((short) 4, Qtypes, Qsizes);
    }
    catch (Exception e) {
      System.err.println("*** error in Tuple.setHdr() ***");
      status = FAIL;
      e.printStackTrace();
    }
    
    for (int i=0; i<numq; i++) {
      try {
	t.setIntFld(1, ((Q)reserves.elementAt(i)).col1);
	t.setIntFld(2, ((Q)reserves.elementAt(i)).col2);
	t.setStrFld(3, ((Q)reserves.elementAt(i)).col3);
	t.setStrFld(4, ((Q)reserves.elementAt(i)).col4);

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
      System.err.println ("*** Error creating relation for Q");
      Runtime.getRuntime().exit(1);
    }
    
  }








  public boolean runTests() {
    
    Disclaimer();
    Query1a();
    Query1b();
 
    
    System.out.print ("Finished joins testing"+"\n");
   
    
    return true;
  }

  private void Query1a_CondExpr(CondExpr[] expr) {

    expr[0].next  = null;
    // We test the "lower than" inequality join
    expr[0].op    = new AttrOperator(AttrOperator.aopLT);
    expr[0].type1 = new AttrType(AttrType.attrSymbol);
    expr[0].type2 = new AttrType(AttrType.attrSymbol);
    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),3);
    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),3);
 
    expr[1] = null;
  }


  private void Query1b_CondExpr(CondExpr[] expr) {

    expr[0].next  = null;
    // We test the "lower than" inequality join
    expr[0].op    = new AttrOperator(AttrOperator.aopLT);
    expr[0].type1 = new AttrType(AttrType.attrSymbol);
    expr[0].type2 = new AttrType(AttrType.attrSymbol);
    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),3);
    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),3);

    expr[1].op    = new AttrOperator(AttrOperator.aopLE);
    expr[1].next  = null;
    expr[1].type1 = new AttrType(AttrType.attrSymbol);
    expr[1].type2 = new AttrType(AttrType.attrInteger);
    expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),4);
    expr[1].operand2.integer = new FldSpec (new RelSpec(RelSpec.innerRel),4);
 
    expr[2] = null;
}









  public void Query1a() {
    
    System.out.print("**********************Query1a strating *********************\n");
    boolean status = OK;
    
    // S, R, Q Queries.
    System.out.print ("Query: \n"
		      + "  SELECT R.col1, S.col1\n"
		      + "  FROM   S, R\n"
		      + "  WHERE  R.col3 < S.col3\n\n");

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

    Query1a_CondExpr(outFilter);
    Tuple t = new Tuple();
    t = null;

    AttrType [] Stypes = {   
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrInteger)
    };
 
    short []   Ssizes = new short[1];
    Ssizes[0] = 0;
 
    AttrType [] Rtypes2 = {  
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrInteger), 
    };
 
    AttrType [] Rtypes = {    
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrInteger), 
    };
 
    short  []  Rsizes = new short[1] ;
    Rsizes[0] = 0;
 
    AttrType [] Stypes2 = {
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrInteger), 
    };
















    short [] JJsize = new short[1];
    JJsize[0] = 30;
    FldSpec []  proj1 = {
       new FldSpec(new RelSpec(RelSpec.outer), 2),
       new FldSpec(new RelSpec(RelSpec.innerRel), 2)
    }; // S.sname, R.bid

    FldSpec [] proj2  = {
       new FldSpec(new RelSpec(RelSpec.outer), 1)
    };
 
    FldSpec [] Sprojection = {
       new FldSpec(new RelSpec(RelSpec.outer), 1),
       new FldSpec(new RelSpec(RelSpec.outer), 2),
       // new FldSpec(new RelSpec(RelSpec.outer), 3),
       // new FldSpec(new RelSpec(RelSpec.outer), 4)
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
      tt.setHdr((short) 4, Stypes, Ssizes);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }

    int sizett = tt.size();
    tt = new Tuple(sizett);
    try {
      tt.setHdr((short) 4, Stypes, Ssizes);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    Heapfile        f = null;
    try {
      f = new Heapfile("sailors.in");
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

    System.out.print ("After Building btree index on sailors.sid.\n\n");
    try {
      am = new IndexScan ( b_index, "sailors.in",
			   "BTreeIndex", Stypes, Ssizes, 4, 2,
			   Sprojection, null, 1, false);
    }
    
    catch (Exception e) {
      System.err.println ("*** Error creating scan for Index scan");
      System.err.println (""+e);
      Runtime.getRuntime().exit(1);
    }
   
    
    NestedLoopsJoins nlj = null;
    try {
      nlj = new NestedLoopsJoins (Stypes2, 2, Ssizes,
				  Rtypes, 3, Rsizes,
				  10,
				  am, "reserves.in",
				  outFilter, null, proj1, 2);
    }
    catch (Exception e) {
      System.err.println ("*** Error preparing for nested_loop_join");
      System.err.println (""+e);
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }

     NestedLoopsJoins nlj2 = null ; 
    try {
      nlj2 = new NestedLoopsJoins (Jtypes, 2, Jsizes,
				   Btypes, 3, Bsizes,
				   10,
				   nlj, "boats.in",
				   outFilter2, null, proj2, 1);
    }
    catch (Exception e) {
      System.err.println ("*** Error preparing for nested_loop_join");
      System.err.println (""+e);
      Runtime.getRuntime().exit(1);
    }
    
    TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
    Sort sort_names = null;
    try {
      sort_names = new Sort (JJtype,(short)1, JJsize,
			     (iterator.Iterator) nlj2, 1, ascending, JJsize[0], 10);
    }
    catch (Exception e) {
      System.err.println ("*** Error preparing for nested_loop_join");
      System.err.println (""+e);
      Runtime.getRuntime().exit(1);
    }
    
    
    QueryCheck qcheck2 = new QueryCheck(2);
    
   
    t = null;
    try {
      while ((t = sort_names.get_next()) != null) {
        t.print(JJtype);
        qcheck2.Check(t);
      }
    }
    catch (Exception e) {
      System.err.println (""+e);
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }

    qcheck2.report(2);

    System.out.println ("\n"); 
    try {
      sort_names.close();
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    
    if (status != OK) {
      //bail out
   
      Runtime.getRuntime().exit(1);
      }
  }


}


