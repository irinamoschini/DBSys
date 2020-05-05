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
import java.io.*;

/**
 *
 *  This file contains an implementation of the self join
 *  algorithm for single predicate as described in the paper.
 *  The algorithm is extremely simple for one single predicate:
 *
 * sort the Tuple's list L according to one attribute and the operator given by the condition
 *      for each Tuple t in L do
 *          for each next tuple s in L do
 *              add (t, s) to the result.
 */

public class SelfJoin1  extends Iterator
{
  private AttrType      _in1[];
  private   int        in1_len;
  private   Iterator  outer;
  private   short t1_str_sizescopy[];
  private   CondExpr OutputFilter[];
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
   *@param in1  Array containing field types of R.
   *@param len_in1  # of columns in R.
   *@param t1_str_sizes shows the length of the string fields.
   *@param amt_of_mem  IN PAGES
   *@param am1  access method for left i/p to join
   *@param outFilter   select expressions
   *@param rightFilter reference to filter applied on right i/p
   *@param proj_list shows what input fields go where in the output tuple
   *@param n_out_flds number of outer relation fileds
   *@exception IOException some I/O fault
   *@exception NestedLoopException exception from this class
 * @throws SortException
 * @throws FieldNumberOutOfBoundException
 * @throws UnknowAttrType
   */
  public SelfJoin1( AttrType    in1[],    
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
     
      outer = am1;
      inner_tuple = new Tuple();
      Jtuple = new Tuple();
      OutputFilter = outFilter;
      RightFilter  = rightFilter;
     
      n_buf_pgs    = amt_of_mem;
      inner = null;
      done  = false;
      get_from_outer = true;
     
      AttrType[] Jtypes = new AttrType[n_out_flds];
      short[]    t_size;
     
      perm_mat = proj_list;
      nOutFlds = n_out_flds;
      joinResult = new ArrayList<Tuple>();
     
     System.out.println("************** Self Join 1 starting **************");
      try {
     t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes,
  in1, len_in1, in1, len_in1,
  t1_str_sizes, t1_str_sizes,
  proj_list, nOutFlds);
      }catch (TupleUtilsException e){
throw new NestedLoopException(e,"TupleUtilsException is caught by NestedLoopsJoins.java");
      }
     
      String operator = outFilter[0].op.toString();
      //Sort sortValues = null;
 int col = OutputFilter[0].operand1.symbol.offset;

      /*
       * L1 and L2 are ArrayLists of ArrayLists to keep the rid of the tuple associated to the attribute value
       */
      ArrayList<ArrayList<Integer>> list1 = new ArrayList<ArrayList<Integer>>();

      Tuple t = null;
      ArrayList<Tuple> tuples = new ArrayList<Tuple>();
   
      int number_tuples = 0;
      int rid = 0;
      try {
         while ((t = outer.get_next()) != null) {
           number_tuples = number_tuples + 1;
           Tuple x = new Tuple(t);
           ArrayList<Integer> temp1 = new ArrayList<Integer>();

           temp1.add(x.getIntFld(col));
           temp1.add(rid);
           list1.add(temp1);
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
     
     
      Comparator<ArrayList<Integer>> comparator_asc = new Comparator<ArrayList<Integer>>() {
     @Override
     public int compare(ArrayList<Integer> o1, ArrayList<Integer> o2) {
         int i = o1.get(0);
         int j = o2.get(0);
         if (i > j) {
             return 1;
         } else if (i < j) {
             return -1;
         } else { // sorting in an increasing rid order in any case
         if (o1.get(1) < o2.get(1))
        {return 1;}
         else if (o1.get(1) > o2.get(1))
      {return -1;}
         else { return 0;}
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
         } else { // sorting in an increasing rid order in any case
         if (o1.get(1) < o2.get(1))
        {return 1;}
         else if (o1.get(1) > o2.get(1))
      {return -1;}
         else { return 0;}
         }
     }
      };
      /*
       * Sorting the list of tuples according to the operator given
       */
 
      //System.out.println("col = " + col);
 
      if((operator.compareTo("aopLT")==0) || (operator.compareTo("aopLE")==0)) {
     /*TupleOrder descending = new TupleOrder(TupleOrder.Descending);
     try {
     sortValues = new Sort(in1,(short)len_in1,t1_str_sizes,outer,col,descending,t1_str_sizes[0],10);
     }
     catch(Exception e) {
     throw new SortException(e,"Error in sorting");
     }
     */
     try {
     Collections.sort(list1, comparator_desc);
     //System.out.println("List1 ordered descending " + list1);
     }
     catch(Exception e) {
     throw new SortException(e,"Error in sorting list 1 ");
     }
      }
      else if((operator.compareTo("aopGT")==0)||(operator.compareTo("aopGE")==0)) {
     /*
     TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
     
     try {
     sortValues = new Sort(in1,(short)len_in1,t1_str_sizes,outer,col,ascending,t1_str_sizes[0],10);
     }
     catch(Exception e) {
     throw new SortException(e,"Error in sorting");
     }
     */
     try {
     Collections.sort(list1, comparator_asc);
     //System.out.println("List1 ordered ascending " + list1);
     }
     catch(Exception e) {
     throw new SortException(e,"Error in sorting list 1 ");
     }
     
      }
     
      /*
       * Transforming the sorted list into ArrayList for simplification
       * For now, we keep the entire tuple (need an improvement for keeping the rids instead)
       */


     
      Tuple tuple1;
      Tuple tuple2;
      int eqOff;
      if (outFilter[0].op.toString() == "aopGE" || outFilter[0].op.toString() == "aopLE") {eqOff = 0;}
      else {eqOff = 1;}
     

      int position;
     
      for (int i = 0; i < number_tuples ; i++) {
     position = i;

 tuple1 = tuples.get(list1.get(position).get(1));
 /*
 System.out.print("tuple 1  "+ tuple1.getIntFld(1));
 System.out.print(" , "+ tuple1.getIntFld(2));
 System.out.print(" , "+ tuple1.getIntFld(3));
 System.out.println(" , "+ tuple1.getIntFld(4));
 */
     for (int k = 0; k < i - eqOff + 1; k++) {
     tuple2 = tuples.get(list1.get(k).get(1));
     
     /*
     System.out.print("tuple 2  "+ tuple2.getIntFld(1));
     System.out.print(" , "+ tuple2.getIntFld(2));
     System.out.print(" , "+ tuple2.getIntFld(3));
     System.out.println(" , "+ tuple2.getIntFld(4));
     */
     
     /*
  * System.out.println("");
  * System.out.println("Tuple1");
  * tuple1.print(_in1);
  * System.out.println("Tuple2");
  * tuple2.print(_in1);
  * System.out.println("EqOff = " + eqOff);
  */
     if (eqOff == 0){
    /*
    *  try {
* System.out.println("Tuples different ? "  + TupleUtils.CompareTupleWithTuple(_in1[col], tuple1, col, tuple2, col));
* } catch (TupleUtilsException e1) {
* // TODO Auto-generated catch block
* e1.printStackTrace();
* }
    *  System.out.println("position = "+position);
    *  System.out.println("k = "+ k);
*  System.out.println("Positions different ? = " + (position != k));
    */
     try {
if((TupleUtils.CompareTupleWithTuple(_in1[col - 1], tuple1, col, tuple2, col) == 0) && (position != k)) {
     Projection.Join(tuple1, _in1,tuple2, _in1,Jtuple, perm_mat, nOutFlds);
     try{  
     Tuple x = new Tuple(Jtuple);
     joinResult.add(x);
      }
     catch(Exception e) {System.out.println("Error ocurred when adding the result tuples");}
       
     Projection.Join(tuple2, _in1,tuple1, _in1,Jtuple, perm_mat, nOutFlds);
     try{  
     Tuple x = new Tuple(Jtuple);
     joinResult.add(x);
      }
     catch(Exception e) {System.out.println("Error ocurred when adding the result tuples");}
 }
 else {
     Projection.Join(tuple1, _in1,tuple2, _in1,Jtuple, perm_mat, nOutFlds);
     try{  
     Tuple x = new Tuple(Jtuple);
     joinResult.add(x);
      }
     catch(Exception e) {System.out.println("Error ocurred when adding the result tuples");}
 }
} catch (TupleUtilsException e) {
// TODO Auto-generated catch block
e.printStackTrace();
}
     }
     if(eqOff == 1){
     try {
if(TupleUtils.CompareTupleWithTuple(_in1[col - 1], tuple1, col, tuple2, col) != 0) {
     Projection.Join(tuple1, _in1,tuple2, _in1,Jtuple, perm_mat, nOutFlds);
     try{  
     Tuple x = new Tuple(Jtuple);
     joinResult.add(x);
      }
     catch(Exception e) {System.out.println("Error ocurred when adding the result tuples");}
   }
} catch (TupleUtilsException e) {
// TODO Auto-generated catch block
e.printStackTrace();
}
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
 throw new JoinsException(e, "SelfJoin1.java: error in closing iterator.");
}
closeFlag = true;
      }
    }
}


