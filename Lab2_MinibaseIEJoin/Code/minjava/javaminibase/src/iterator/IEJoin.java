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

public class IEJoin  extends Iterator 
{
  private AttrType      _in1[];
  private   int        in1_len;
  private AttrType      _in2[];
  private   int        in2_len;

  private   Iterator  outer;
  private   Iterator  inner;

  private   short t1_str_sizescopy[];
  private   short t2_str_sizescopy[];
  private   CondExpr OutputFilter[];		// we will need to modify the output filter to take account of 2 predicates
  private   CondExpr RightFilter[];
  private   int        n_buf_pgs;        // # of buffer pages available.
  private   boolean        done,         // Is the join complete
    get_from_outer;                 // if TRUE, a tuple is got from outer
  private   Tuple     outer_tuple, inner_tuple;
  private   Tuple     Jtuple;           // Joined tuple
  private   FldSpec   perm_mat[];
  private   int        nOutFlds;
  private ArrayList<Tuple> joinResult;
  
  
  /**constructor
   *Initialize the single relation which
   *@param in1  Array containing field types of R.
   *@param len_in1  # of columns in R.
   *@param t1_str_sizes shows the length of the string fields.
   *@param in2  Array containing field types of Q.
   *@param len_in2  # of columns in Q.
   *@param t2_str_sizes shows the length of the string fields.
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
  public IEJoin( AttrType    in1[],    
			   int     len_in1,           
			   short   t1_str_sizes[],
			   AttrType    in2[],    
			   int     len_in2,           
			   short   t2_str_sizes[],
			   int     amt_of_mem,      
			   Iterator     am1,  
			   Iterator     am2,          
			   CondExpr outFilter[],      
			   CondExpr rightFilter[],    
			   FldSpec   proj_list[],
			   int        n_out_flds
			   ) throws IOException,NestedLoopException, SortException, UnknowAttrType, FieldNumberOutOfBoundException
    {
      
      _in1 = new AttrType[in1.length];
      _in2 = new AttrType[in2.length];
      System.arraycopy(in1,0,_in1,0,in1.length);
      System.arraycopy(in2,0,_in2,0,in2.length);
      in1_len = len_in1;
      in2_len = len_in2;
      

      outer = am1;
      inner = am2;
      t2_str_sizescopy =  t2_str_sizes;
      inner_tuple = new Tuple();
      Jtuple = new Tuple();
      OutputFilter = outFilter;
      int outfilter_len = outFilter.length;
      RightFilter  = rightFilter;
      
      n_buf_pgs    = amt_of_mem;
      done  = false;
      get_from_outer = true;      
      
      AttrType[] Jtypes = new AttrType[n_out_flds];
      short[]    t_size;
      
      perm_mat = proj_list;
      nOutFlds = n_out_flds;
      joinResult = new ArrayList<Tuple>();
      try {
	t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes,
					   in1, len_in1, in2, len_in2,
					   t1_str_sizes, t2_str_sizes,
					   proj_list, nOutFlds);
      }catch (TupleUtilsException e){
	throw new NestedLoopException(e,"TupleUtilsException is caught by NestedLoopsJoins.java");
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
       * OUTER RELATION
       * L1 and L2 are ArrayLists of ArrayLists to keep the rid of the tuple associated to the attribute value 
       */
      
      ArrayList<ArrayList<Integer>> list1 = new ArrayList<ArrayList<Integer>>();

      ArrayList<ArrayList<Integer>> list2 = new ArrayList<ArrayList<Integer>>();

      Tuple t1 = null;
      ArrayList<Tuple> outer_tuples = new ArrayList<Tuple>();

   
      int number_outer_tuples = 0;
      int rid_outer = 0;
      try {
         while ((t1 = outer.get_next()) != null) {
           number_outer_tuples = number_outer_tuples + 1;
           Tuple x = new Tuple(t1);
           ArrayList<Integer> temp1 = new ArrayList<Integer>();
           ArrayList<Integer> temp2 = new ArrayList<Integer>();

           temp1.add(x.getIntFld(col1));
           temp1.add(x.getIntFld(col2));

           temp1.add(rid_outer);
           list1.add(temp1);
           
           temp2.add(x.getIntFld(col2));
           temp2.add(x.getIntFld(col1));

           temp2.add(rid_outer);
           list2.add(temp2);
           outer_tuples.add(x);
           
           /*
     	   System.out.print("outer tuple :  "+ x.getIntFld(1));
     	   System.out.print(" , " + x.getIntFld(2));
     	   System.out.print(" , " + x.getIntFld(3));
     	   System.out.println(" , " +  x.getIntFld(4));
     	   */
           
     	   rid_outer = rid_outer + 1; 
         }
       }
       catch (Exception e) {
         System.err.println (""+e);
         e.printStackTrace();
         Runtime.getRuntime().exit(1);
       }
      
      /*
       * INNER RELATION
       * L1 and L2 are ArrayLists of ArrayLists to keep the rid of the tuple associated to the attribute value 
       */
      
      ArrayList<ArrayList<Integer>> list1prime = new ArrayList<ArrayList<Integer>>();
      ArrayList<ArrayList<Integer>> list2prime = new ArrayList<ArrayList<Integer>>();

      Tuple t2 = null;
      ArrayList<Tuple> inner_tuples = new ArrayList<Tuple>();

   
      int number_inner_tuples = 0;
      int rid_inner = 0;
      try {
         while ((t2 = inner.get_next()) != null) {
           number_inner_tuples = number_inner_tuples + 1;
           Tuple x = new Tuple(t2);
           ArrayList<Integer> temp1 = new ArrayList<Integer>();
           ArrayList<Integer> temp2 = new ArrayList<Integer>();

           temp1.add(x.getIntFld(col1));
           temp1.add(x.getIntFld(col2));

           temp1.add(rid_inner);
           list1prime.add(temp1);
           
           temp2.add(x.getIntFld(col2));
           temp2.add(x.getIntFld(col1));

           temp2.add(rid_inner);
           list2prime.add(temp2);
           inner_tuples.add(x);
           
           /*
     	   System.out.print("inner tuple :  "+ x.getIntFld(1));
     	   System.out.print(" , " + x.getIntFld(2));
     	   System.out.print(" , " + x.getIntFld(3));
     	   System.out.println(" , " +  x.getIntFld(4));
     	   */
     	   rid_inner = rid_inner + 1; 
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
      Comparator<ArrayList<Integer>> comparator_asc = new Comparator<ArrayList<Integer>>() {
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
      
      Comparator<ArrayList<Integer>> comparator_desc = new Comparator<ArrayList<Integer>>() {
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
      
      /*
       * Sorting the lists of the first operator : L1 and L1'
       */
      if(((operator1.compareTo("aopGT")==0) || (operator1.compareTo("aopGE")==0)) )
    		  {
    	  try {
    		  Collections.sort(list1, comparator_desc);
    		  //System.out.println("List1 ordered descending " + list1);
    	  }
    	  catch(Exception e) {
    		  throw new SortException(e,"Error in sorting list 1 ");
    	  }
    	  
    	  try {
    		  Collections.sort(list1prime, comparator_desc);
    		  //System.out.println("List1prime ordered descending " + list1prime);
    	  }
    	  catch(Exception e) {
    		  throw new SortException(e,"Error in sorting list 1 prime ");
    	  }
      }
      
      else  {
		  try {
			  Collections.sort(list1, comparator_asc);
			  //System.out.println("List1 ordered ascending " + list1);
		  }
		  catch(Exception e) {
			  throw new SortException(e,"Error in sorting list 1 ");
		  }
		  
		  try {
			  Collections.sort(list1prime, comparator_asc);
			  //System.out.println("List1prime ordered ascending " + list1prime);
		  }
		  catch(Exception e) {
			  throw new SortException(e,"Error in sorting list 1 prime ");
		  }
      }
      
      /*
       * Sorting the lists of the second operator : L2 and L2'
       */
      if(((operator2.compareTo("aopGT")==0) || (operator2.compareTo("aopGE")==0)) )
    		  {
    	  try {
    		  Collections.sort(list2, comparator_asc);
    		  //System.out.println("List2 ordered ascending " + list2);
    	  }
    	  catch(Exception e) {
    		  throw new SortException(e,"Error in sorting list 2 ");
    	  }
    	  
    	  try {
    		  Collections.sort(list2prime, comparator_asc);
    		  //System.out.println("List2prime ordered ascending " + list2prime);
    	  }
    	  catch(Exception e) {
    		  throw new SortException(e,"Error in sorting list 2 prime ");
    	  }
    	  
    	  
    	  
      }
      
      else  {
		  try {
			  Collections.sort(list2, comparator_desc);
			  //System.out.println("List2 ordered descending " + list2);
		  }
		  catch(Exception e) {
			  throw new SortException(e,"Error in sorting list 2 ");
		  }
		  
		  try {
			  Collections.sort(list2prime, comparator_desc);
			  //System.out.println("List2prime ordered descending " + list2prime);
		  }
		  catch(Exception e) {
			  throw new SortException(e,"Error in sorting list 2 prime ");
		  }
      }     
 

      // Initialization of the Bit Array B'
      int[] bitArrayPrime = new int[number_inner_tuples];
      for (int m = 0; m < number_inner_tuples; m++) 
      {
    	  bitArrayPrime[m] = 0;
      }
            
      // Initialization of the Permutation Array P
      int[] permutationArray = new int[number_outer_tuples];
      
      Tuple tuple1 = null;
      Tuple tuple2 = null;

      for (int i=0; i < number_outer_tuples ;i++)  {
    	  int rid_temp = list2.get(i).get(2);
    	  int j = 0; 
    	  while(list1.get(j).get(2) != rid_temp) {j = j+1;}
    	  permutationArray[i] = j; 
      }

      /*
      System.out.println("permutationArray");
      for(int i = 0; i<number_outer_tuples;i++) {
    	  System.out.println(permutationArray[i]);
      }
      */
      
      // Initialization of the Permutation Array P'
      int[] permutationArrayPrime = new int[number_inner_tuples];
      
      Tuple tuple1prime = null;
      Tuple tuple2prime = null;

      for (int i=0; i < number_inner_tuples ;i++)  {
    	  int rid_temp = list2prime.get(i).get(2);
    	  int j = 0; 
    	  while(list1prime.get(j).get(2) != rid_temp) {j = j+1;}
    	  permutationArrayPrime[i] = j; 
      }

      /*
      System.out.println("permutationArray Prime");
      for(int i = 0; i<number_inner_tuples;i++) {
    	  System.out.println(permutationArrayPrime[i]);
      }
      */

      // Initialization of the Offset Array O1
      int[] offsetArray1 = new int[number_outer_tuples];
      
      for (int i = 0; i < number_outer_tuples; i++) {
          //System.out.println("i = " + i);

    	  // case in L1' is sorted by descending order
          if(((operator1.compareTo("aopGT")==0) || (operator1.compareTo("aopGE")==0)) ) {
        	  int j = number_inner_tuples -1 ;
        	  while (list1prime.get(j).get(0) < list1.get(i).get(0) && j > 0) { 
        		  j--;
        		  }

        	  offsetArray1[i] = j;
          }
          else {
        	  int j = 0;

        	  while (list1prime.get(j).get(0) < list1.get(i).get(0) && j < number_inner_tuples - 1) { 
        	      //System.out.println("j = " + j);

        		  j++;}
        	  offsetArray1[i] = j;
          }
      }

      /*
      System.out.println("Offset Array 1");
      for(int i = 0; i<number_outer_tuples;i++) {
    	  System.out.println(offsetArray1[i]);
      }
      */
      
      // Initialization of the Offset Array O2
      int[] offsetArray2 = new int[number_outer_tuples];

      for (int i = 0; i < number_outer_tuples; i++) {
    	  // case in L2' is sorted by descending order
          if(((operator2.compareTo("aopLT")==0) || (operator2.compareTo("aopLE")==0)) ) {
        	  int j = number_inner_tuples - 1;
        	  while (list2prime.get(j).get(0) < list2.get(i).get(0) && j > 0) { j--;}
        	  offsetArray2[i] = j;
          }
          else {
        	  int j = 0;
        	  while (list2prime.get(j).get(0) < list2.get(i).get(0) && j < number_inner_tuples - 1) { 
        		  j++;}
        	  offsetArray2[i] = j;
          }
      }
      
      /*
      System.out.println("Offset Array 2");
      for(int i = 0; i<number_outer_tuples;i++) {
    	  System.out.println(offsetArray2[i]);
      }
      */
            
      for (int i = 0; i < number_outer_tuples ; i++) {
		  //System.out.println("  ");
		  int off2 = offsetArray2[i];
    	  for (int j = 0; j < Math.min(off2, number_inner_tuples); j++) {
    		  bitArrayPrime[permutationArrayPrime[j]] = 1; 
    	  }
    	  int off1 = offsetArray1[permutationArray[i]];
    	  tuple1 = outer_tuples.get(list2.get(i).get(2));
    	  /*
		  System.out.print("tuple 1  "+ tuple1.getIntFld(1));
		  System.out.print(" , "+ tuple1.getIntFld(2));
		  System.out.print(" , "+ tuple1.getIntFld(3));
		  System.out.println(" , "+ tuple1.getIntFld(4));
		  */
    	  for (int k = off1 + eqOff; k < number_inner_tuples; k++) {
    		  /*
    	      System.out.println("Bit Array Prime");
    	      for(int p = 0; p<number_outer_tuples;i++) {
    	    	  System.out.println(bitArrayPrime[p]);
    	      }
    	      */
    		  if (bitArrayPrime[k] == 1) {
    			  tuple2 = inner_tuples.get(list1prime.get(k).get(2));
    			  /*
    			  System.out.print("tuple 2  "+ tuple2.getIntFld(1));
    			  System.out.print(" , "+ tuple2.getIntFld(2));
    			  System.out.print(" , "+ tuple2.getIntFld(3));
    			  System.out.println(" , "+ tuple2.getIntFld(4));
    			  */
			      Projection.Join(tuple1, _in1,tuple2, _in1,Jtuple, perm_mat, nOutFlds);
			      try{  
			    	  Tuple x = new Tuple(Jtuple);
			    	  joinResult.add(x);
			      		}
			      catch(Exception e) {System.out.println("Error ocurred when adding the result tuples");}
    		  		}
    	  }
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





