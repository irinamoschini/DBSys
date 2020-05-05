package iterator;
   

import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import index.*;
import java.lang.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.io.*;

/** 
 *
 *  This file contains an implementation of the self join
 *  algorithm for two predicates predicate as described in the paper.
 *  The algorithm more complex for double predicates rather than single predicates:
 *
 *		sort the 2 Tuple's list L1 and L2 according to the operators given by the conditions
 *		While sorting, compute the permutation array P of L2 w.r.t L1
 *		Initialization of bit-array to 0
 *      for i in range len(P) do
 *      	B[P[i]] = 1
 *          for k > P[i], scan B from left to right 
 *          	if B[k] = 1, do 
 *              	add (L1[k], L2[i]) to the result.
 */

public class SelfJoin2  extends Iterator 
{
  private AttrType      _in1[];
  private   int        in1_len;
  private   Iterator  outer;

  private   short t1_str_sizescopy[];
  private   CondExpr OutputFilter[];		// we will need to modify the output filter to take account of 2 predicates
  private   CondExpr RightFilter[];
  private   int        n_buf_pgs;        // # of buffer pages available.
  private   boolean        done,         // Is the join complete
    get_from_outer;                 // if TRUE, a tuple is got from outer
  private   Tuple     outer_tuple, inner_tuple;
  private   Tuple     Jtuple;           // Joined tuple
  private   FldSpec   perm_mat[];
  private   int        nOutFlds;
  private   Scan      inner;
  private ArrayList<Tuple> joinResult;
  
  
  /**constructor
   *Initialize the single relation which
   *@param in1  Array containing field types of Q.
   *@param len_in1  # of columns in R.
   *@param t1_str_sizes shows the length of the string fields.
   *@param amt_of_mem  IN PAGES
   *@param am1  access method for left i/p to join
   *@param outFilter   select expressions
   *@param rightFilter reference to filter applied on right i/p
   *@param proj_list shows what input fields go where in the output tuple
   *@param n_out_flds number of outer relation fields
   *@exception IOException some I/O fault
   *@exception NestedLoopException exception from this class
 * @throws SortException 
 * @throws FieldNumberOutOfBoundException 
 * @throws UnknowAttrType 
   */
  public SelfJoin2( AttrType    in1[],    
			   int     len_in1,           
			   short   t1_str_sizes[],
			   int     amt_of_mem,        
			   Iterator     am1,         
			   CondExpr outFilter[],      
			   CondExpr rightFilter[],    
			   FldSpec   proj_list[],
			   int        n_out_flds
			   ) throws IOException,NestedLoopException, SortException, UnknowAttrType, FieldNumberOutOfBoundException
    {
      
      _in1 = new AttrType[in1.length];
      System.arraycopy(in1,0,_in1,0,in1.length);
      in1_len = len_in1;
      int outfilter_len = outFilter.length;
      //System.out.println("Lenght outfilter = " + outfilter_len);

      outer = am1;
      inner_tuple = new Tuple();
      Jtuple = new Tuple();
      OutputFilter = outFilter;
      RightFilter  = rightFilter;
      
      n_buf_pgs    = amt_of_mem;
      inner = null;
      done  = false;
      get_from_outer = true;
      
      System.out.println("************** Self Join 2 starting **************");

      AttrType[] Jtypes = new AttrType[n_out_flds];
      short[]    t_size;
      
      perm_mat = proj_list;
      nOutFlds = n_out_flds;
      joinResult = new ArrayList<Tuple>();
      
     
      try {
    	  t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes,
					   in1, len_in1, in1, len_in1,
					   t1_str_sizes, t1_str_sizes,
					   proj_list, nOutFlds);
      }catch (TupleUtilsException e){
	throw new NestedLoopException(e,"TupleUtilsException is caught by SelfJoin2.java");
      }
       
      /*
       * Creating the two lists containing the values of the attributes
       * under constraints
       */
      String operator1 = outFilter[0].op.toString();
	  int col1 = OutputFilter[0].operand1.symbol.offset;
      String operator2 = new String("");
      int col2 = 0;

      // By default
      int eqOff = 1;
      if(outfilter_len == 3) 
          // The second condition is linked with the first one by 'AND'
      {
          operator2 = outFilter[1].op.toString();
          col2 = OutputFilter[1].operand1.symbol.offset;
          if ((outFilter[0].op.toString() == "aopGE" || outFilter[0].op.toString() == "aopLE") && (outFilter[1].op.toString() == "aopGE" || outFilter[1].op.toString() == "aopLE"))
          	{eqOff = 0;}
      }
      else {
    	// The second condition is linked with the first one by 'OR'
          operator2 = outFilter[0].next.op.toString();
          col2 = OutputFilter[0].next.operand1.symbol.offset;
          if ((outFilter[0].op.toString() == "aopGE" || outFilter[0].op.toString() == "aopLE") && (outFilter[0].next.op.toString() == "aopGE" || outFilter[0].next.op.toString() == "aopLE"))
          	{eqOff = 0;}
      }
      
      
      /*
       * L1 and L2 are ArrayLists of ArrayLists to keep the rid of the tuple associated to the attribute value 
       */
      ArrayList<ArrayList<Integer>> list1 = new ArrayList<ArrayList<Integer>>();
      ArrayList<ArrayList<Integer>> list2 = new ArrayList<ArrayList<Integer>>();

      Tuple t = null;
      ArrayList<Tuple> tuples = new ArrayList<Tuple>();
   
      int number_tuples = 0;
      int rid = 0;
      try {
         while ((t = outer.get_next()) != null) {
           number_tuples = number_tuples + 1;
           Tuple x = new Tuple(t);
           ArrayList<Integer> temp1 = new ArrayList<Integer>();
           ArrayList<Integer> temp2 = new ArrayList<Integer>();

           temp1.add(x.getIntFld(col1));
           temp1.add(x.getIntFld(col2));

           temp1.add(rid);
           list1.add(temp1);
           
           temp2.add(x.getIntFld(col2));
           temp2.add(x.getIntFld(col1));

           temp2.add(rid);
           list2.add(temp2);
           tuples.add(x);
           
           /*
     	   System.out.print("tuple :  "+ x.getIntFld(1));
     	   System.out.print(" , " + x.getIntFld(2));
     	   System.out.print(" , " + x.getIntFld(3));
     	   System.out.println(" , " +  x.getIntFld(4));
     	   */
     	   rid = rid + 1; 
         }
       }
       catch (Exception e) {
         System.err.println (""+e);
         e.printStackTrace();
         Runtime.getRuntime().exit(1);
       }
      

      /*
       * To sort L1 and L2 which are ArrayLists of ArrayLists, we custom our own comparator method 
       * to sort according to the first column, that is to say order the attribute values 
       * and keeping the rid associated. 
       */
      Comparator<ArrayList<Integer>> comparator_asc_asc = new Comparator<ArrayList<Integer>>() {
    	  @Override
    	  public int compare(ArrayList<Integer> o1, ArrayList<Integer> o2) {
    	      int i = o1.get(0);
    	      int j = o2.get(0);
    	      if (i > j) {
    	          return 1;
    	      } else if (i < j) {
    	          return -1;
    	      } else {
    	    	  if (o1.get(1) > o2.get(1))
    	    	  	{return 1;}
    	    	  else if (o1.get(1) < o2.get(1))
  	    	  		{return -1;}
    	    	  else { // in any case, we sort by ascending order the rids if both attributes are duplicates   	    	  
    	    		  if (o1.get(2) > o2.get(2))
    	    		  {return 1;}
    	    		  else if (o1.get(2) < o2.get(2))
	    	  			{return -1;}
    	    		  else {
    	    			  return 0; 
    	    		  } 
    	    	  }  
    	      }
    	  }
      };

      Comparator<ArrayList<Integer>> comparator_asc_desc = new Comparator<ArrayList<Integer>>() {
    	  @Override
    	  public int compare(ArrayList<Integer> o1, ArrayList<Integer> o2) {
    	      int i = o1.get(0);
    	      int j = o2.get(0);
    	      if (i > j) {
    	          return 1;
    	      } else if (i < j) {
    	          return -1;
    	      } else {
    	    	  if (o1.get(1) < o2.get(1))
  	    	  	{return 1;}
    	    	  else if (o1.get(1) > o2.get(1))
	    	  		{return -1;}
    	    	  else { // in any case, we sort by ascending order the rids if both attributes are duplicates   	  
    	    		  if (o1.get(2) < o2.get(2))
  	    		  {return 1;}
    	    		  else if (o1.get(2) > o2.get(2))
	    	  			{return -1;}
  	    		  else {
  	    			  return 0;
  	    		  } 
  	    	  }  
  	      }
    	  }
      };
      
      Comparator<ArrayList<Integer>> comparator_desc_asc = new Comparator<ArrayList<Integer>>() {
    	  @Override
    	  public int compare(ArrayList<Integer> o1, ArrayList<Integer> o2) {
    	      int i = o1.get(0);
    	      int j = o2.get(0);
    	      if (i < j) {
    	          return 1;
    	      } else if (i > j) {
    	          return -1;
    	      } else {
    	    	  if (o1.get(1) > o2.get(1))
    	    	  	{return 1;}
    	    	  else if (o1.get(1) < o2.get(1))
  	    	  		{return -1;}
    	    	  else { // in any case, we sort by ascending order the rids if both attributes are duplicates   	    	  
    	    		  if (o1.get(2) > o2.get(2))
    	    		  {return 1;}
    	    		  if (o1.get(2) < o2.get(2))
	    	  			{return -1;}
    	    		  else {
    	    			  return 0;
    	    		  } 
    	    	  }  
    	      }
    	  }
      };
      
      Comparator<ArrayList<Integer>> comparator_desc_desc = new Comparator<ArrayList<Integer>>() {
    	  @Override
    	  public int compare(ArrayList<Integer> o1, ArrayList<Integer> o2) {
    	      int i = o1.get(0);
    	      int j = o2.get(0);
    	      if (i < j) {
    	          return 1;
    	      } else if (i > j) {
    	          return -1;
    	      } else {
    	    	  if (o1.get(1) < o2.get(1))
  	    	  	{return 1;}
    	    	  else if (o1.get(1) > o2.get(1))
	    	  		{return -1;}
  	    	  else { // in any case, we sort by ascending order the rids if both attributes are duplicates   	  
  	    		  if (o1.get(2) > o2.get(2))
  	    		  {return 1;}
  	    		  else if (o1.get(2) < o2.get(2))
	    	  			{return -1;}
  	    		  else {
  	    			  return 0;
  	    		  } 
  	    	  }  
  	      }
    	  }
      };
   
      
      
      /*
       * Sorting the first list according to the operator given
       */
      if(((operator1.compareTo("aopGT")==0) && ( (operator2.compareTo("aopLE")==0) || (operator2.compareTo("aopLT")==0)) )
    		  ||
    		  ((operator1.compareTo("aopGE")==0) && ( (operator2.compareTo("aopGT")==0) || (operator2.compareTo("aopGE")==0)) ))
    		  {
    	  try {
    		  Collections.sort(list1, comparator_asc_asc);
    		  //System.out.println("List1 ordered ascending ascending " + list1);
    	  }
    	  catch(Exception e) {
    		  throw new SortException(e,"Error in sorting list 1 ");
    	  }
      }
      
      if(((operator1.compareTo("aopGT")==0) && ( (operator2.compareTo("aopGT")==0) || (operator2.compareTo("aopGE")==0)) )
    		  ||
    		  ((operator1.compareTo("aopGE")==0) && ( (operator2.compareTo("aopLT")==0) || (operator2.compareTo("aopLE")==0)) ))
      {
    	  try {
    		  Collections.sort(list1, comparator_asc_desc);

    		  //System.out.println("List1 ordered ascending descending " + list1);
    	  }
    	  catch(Exception e) {
    		  throw new SortException(e,"Error in sorting list 1 ");
    	  }
      }
      if(((operator1.compareTo("aopLT")==0) && ( (operator2.compareTo("aopLT")==0) || (operator2.compareTo("aopLE")==0)) )
    		  ||
    		  ((operator1.compareTo("aopLE")==0) && ( (operator2.compareTo("aopGT")==0) || (operator2.compareTo("aopGE")==0)) ))
      		{
    	  try {
    		  Collections.sort(list1, comparator_desc_asc);
    		  //System.out.println("List1 ordered descending ascending " + list1);
    	  }
    	  catch(Exception e) {
    		  throw new SortException(e,"Error in sorting list 1 ");
    	  }
      }
      else if(((operator1.compareTo("aopLT")==0) && ( (operator2.compareTo("aopGT")==0) || (operator2.compareTo("aopGE")==0)) )
    		  ||
    		  ((operator1.compareTo("aopLE")==0) && ( (operator2.compareTo("aopLT")==0) || (operator2.compareTo("aopLE")==0)) )) {
    	  try {
    		  Collections.sort(list1, comparator_desc_desc);
    		  //System.out.println("List1 ordered descending descending " + list1);
    	  }
    	  catch(Exception e) {
    		  throw new SortException(e,"Error in sorting list 1 ");
    	  }
      }

      
      
      /*
       * Sorting the second list of tuples according to the operator given
       * Two different cases to have access to the second operator : 
       * 	- if len(output_filter) = 3, we have an 'AND' and we have access to the second operator thanks to outFilter[1].op.toString()
       * 	- if len(output_filter) = 2, we have an 'OR' and we have access to the second operator thanks to outFilter[0].next.op.toString()
       */
      if(((operator2.compareTo("aopLT")==0) && ( (operator1.compareTo("aopGT")==0) || (operator1.compareTo("aopGE")==0)) )
    		  ||
    		  ((operator2.compareTo("aopLE")==0) && ( (operator1.compareTo("aopLT")==0) || (operator1.compareTo("aopLE")==0)) ))
    		  {
    	  try {
    		  Collections.sort(list2, comparator_asc_asc);

    		  //System.out.println("List2 ordered ascending ascending " + list2);
    	  }
    	  catch(Exception e) {
    		  throw new SortException(e,"Error in sorting list 2 ");
    	  }
      }
      
      if(((operator2.compareTo("aopGT")==0) && ( (operator1.compareTo("aopGT")==0) || (operator1.compareTo("aopGE")==0)) )
    		  ||
    		  ((operator2.compareTo("aopGE")==0) && ( (operator1.compareTo("aopLT")==0) || (operator1.compareTo("aopLE")==0)) ))
      {
    	  try {
    		  Collections.sort(list2, comparator_asc_desc);
    		  //System.out.println("List2 ordered ascending descending " + list2);
    	  }
    	  catch(Exception e) {
    		  throw new SortException(e,"Error in sorting list 2 ");
    	  }
      }
      if(((operator2.compareTo("aopLT")==0) && ( (operator1.compareTo("aopLT")==0) || (operator1.compareTo("aopLE")==0)) )
    		  ||
    		  ((operator2.compareTo("aopLE")==0) && ( (operator1.compareTo("aopGT")==0) || (operator1.compareTo("aopGE")==0)) ))
      		{
    	  try {
    		  Collections.sort(list2, comparator_desc_asc);
    		  //System.out.println("List2 ordered descending ascending " + list2);
    	  }
    	  catch(Exception e) {
    		  throw new SortException(e,"Error in sorting list 2 ");
    	  }
      }
      else if(((operator2.compareTo("aopGT")==0) && ( (operator1.compareTo("aopLT")==0) || (operator1.compareTo("aopLE")==0)) )
    		  ||
    		  ((operator2.compareTo("aopGE")==0) && ( (operator1.compareTo("aopGT")==0) || (operator1.compareTo("aopGE")==0)) )) {
    	  try {
    		  Collections.sort(list2, comparator_desc_desc);
    		  //System.out.println("List2 ordered descending descending " + list2);
    	  }
    	  catch(Exception e) {
    		  throw new SortException(e,"Error in sorting list 2 ");
    	  }
      }
      

      // Initialization of the Bit Array
      int[] bitArray = new int[number_tuples];
      for (int m = 0; m < number_tuples; m++) 
      {
    	  bitArray[m] = 0;
      }
            
      // Initialization of the Permutation Array
      int[] permutationArray = new int[number_tuples];
      
      Tuple tuple1 = null;
      Tuple tuple2 = null;

      for (int i=0; i < number_tuples ;i++)  {
    	  int rid_temp = list2.get(i).get(2);
    	  int j = 0; 
    	  while(list1.get(j).get(2) != rid_temp) {j = j+1;}
    	  permutationArray[i] = j; 
      }

      /*
      System.out.println("permutationArray");
      for(int i = 0; i<number_tuples;i++) {
    	  System.out.println(permutationArray[i]);
      }
      */
      
      for (int i = 0; i < number_tuples ; i++) {
		  //System.out.println("  ");
    	  int pos = permutationArray[i];
    	  bitArray[pos] = 1;
		  tuple2 = tuples.get(list2.get(i).get(2));
		  /*
		  System.out.print("tuple 2  "+ tuple2.getIntFld(1));
		  System.out.print(" , "+ tuple2.getIntFld(2));
		  System.out.print(" , "+ tuple2.getIntFld(3));
		  System.out.println(" , "+ tuple2.getIntFld(4));
		  */
    	  for (int k = pos + eqOff; k < number_tuples; k++) {
    		  if (bitArray[k] == 1) {
        		  tuple1 = tuples.get(list1.get(k).get(2));
        		  /*
        		  System.out.print("tuple 1  "+ tuple1.getIntFld(1));
        		  System.out.print(" , "+ tuple1.getIntFld(2));
        		  System.out.print(" , "+ tuple1.getIntFld(3));
        		  System.out.println(" , "+ tuple1.getIntFld(4));
        		  */
			      Projection.Join(tuple1, _in1,tuple2, _in1,Jtuple, perm_mat, nOutFlds);
			      try{  
			    	  Tuple x = new Tuple(Jtuple);
			    	  joinResult.add(x);
			      		}
			      catch(Exception e) {System.out.println("Error ocurred when adding the result tuples");}
    		  		}
    	  }
    	  //System.out.println("end of iteration i  " + i);

      }
     
    } 
  
  /**  
   *@return The joined tuple is returned
   *@exception IOException I/O errors
   *@exception JoinsException some join exception
   *@exception IndexException exception from super class
   *@exception InvalidTupleSizeException invalid tuple size
   *@exception InvalidTypeException tuple type not valid
   *@exception PageNotReadException exception from lower layer
   *@exception TupleUtilsException exception from using tuple utilities
   *@exception PredEvalException exception from PredEval class
   *@exception SortException sort exception
   *@exception LowMemException memory error
   *@exception UnknowAttrType attribute type unknown
   *@exception UnknownKeyTypeException key type unknown
   *@exception Exception other exceptions
   */
  
  public Tuple get_next()
    throws 
	   Exception
    {
      Tuple t = new Tuple();
      try {
    	  while (joinResult.size()!=0) {
    		  t = joinResult.get(0);
    		  joinResult.remove(0);
    		  return t;
    		  
    	  }
      }
      catch(Exception e) {
    	  System.out.println("Error in getting next tuple");
    	 
      }
      return null;
    } 
 
  /**
   * implement the abstract method close() from super class Iterator
   *to finish cleaning up
   *@exception IOException I/O error from lower layers
   *@exception JoinsException join error from lower layers
   *@exception IndexException index access error 
   */
  public void close() throws JoinsException, IOException,IndexException 
    {
      if (!closeFlag) {
	
	try {
	  outer.close();
	}catch (Exception e) {
	  throw new JoinsException(e, "SelfJoin2.java: error in closing iterator.");
	}
	closeFlag = true;
      }
    }
  
  
  

 


}





