/*
 * CrossIterator.java
 *
 * DBMS Implementation
 */

import java.io.*;
import com.sleepycat.db.*;
import java.util.*;

/**
 * A class that serves as an iterator over some or all of the tuples
 * in the cross product (i.e., Cartesian product) of two or more
 * tables.
 */
public class CrossIterator extends RelationIterator {
    private TableIterator[] tableIter;
    private Column[] columns;
    private ConditionalExpression where;
    private int numTuples;
    
    /**
     * Constructs a CrossIterator object for the subset of the cross
     * product specified by the given SQLStatement.  If the
     * SQLStatement has a WHERE clause, the iterator will only visit
     * rows that satisfy the WHERE clause.
     *
     * @param  stmt  the SQL statement that specifies the cross product
     * @throws IllegalStateException if one of the Table objects in stmt
     *         has not already been opened
     * @throws DatabaseException if Berkeley DB encounters a problem
     *         while accessing one of the underlying database(s)
     */
    public CrossIterator(SQLStatement stmt) throws DatabaseException {
        /* not yet implemented */
      init = 0;
 
 // make table iterator array
  int tableCount = stmt.numTables();
  tableIter = new TableIterator[tableCount];

  for(int i=0; i<tableCount; i++){
    TableIterator ti = new TableIterator(stmt, stmt.getTable(i), false);
  //ti.next();
  //ti.first();
    tableIter[i]= ti; 
  }

 // make column array
  ArrayList<Column> colList = new ArrayList<Column>();
  for(int i=0; i<tableCount; i++){
    Table t = stmt.getTable(i);
    int numCol = t.numColumns();
    for(int j=0; j<numCol; j++){
      Column c = t.getColumn(j);
   c.setTableIterator(tableIter[i]);
      colList.add(c);
    }
  }

 columns = new Column[colList.size()];
 for(int i=0; i<colList.size(); i++){
  columns[i] = colList.get(i);
 }

 //System.out.println(columns[i].getValue());

 this.where = stmt.getWhere();
 if(this.where == null) this.where = new TrueExpression();
 
 //this.where = (evalWhere ? stmt.getWhere() : null);
 //       if (this.where == null)
 //           this.where = new TrueExpression();
 
 

 numTuples = 0;

 //System.out.println("end of constructor/n");
 


    }
    
    /**
     * Closes the iterator, which closes any BDB handles that it is using.
     *
     * @throws DatabaseException if Berkeley DB encounters a problem
     *         while closing a handle
     */
    public void close() throws DatabaseException {
        /* not yet implemented */

 for(int i=0; i<tableIter.length; i++){
  tableIter[i].close();

 }

    }
    
    /**
     * Advances the iterator to the next tuple in the relation.  If
     * there is a WHERE clause that limits which tuples should be
     * included in the relation, this method will advance the iterator
     * to the next tuple that satisfies the WHERE clause.  If the
     * iterator is newly created, this method will position it on the first
     * tuple in the relation (that satisfies the WHERE clause).
     *
     * @return true if the iterator was advanced to a new tuple, and false
     *         if there are no more tuples to visit
     * @throws DeadlockException if deadlock occurs while accessing the
     *         underlying BDB database(s)
     * @throws DatabaseException if Berkeley DB encounters another problem
     *         while accessing the underlying database(s)
     */
    
    private int init;
    
    public boolean next() throws DeadlockException, DatabaseException {
        /* not yet implemented */
      init++;
      

 if(init == 1){
   for(int i=tableIter.length - 1; i>=0; i--){
     if(!tableIter[i].next()) return false;
   }
  
  if(this.where.isTrue()){
    numTuples++;
    return true;
  }
 } 

 boolean repeat = true;
 
 while(repeat){
   for(int i=tableIter.length - 1; i>=0; i--){
     // advance iterator and leave for loop  
     if(tableIter[i].next()) break;
  
     // reset iterator to first tuple that satisfies where clause
     //int beforeReset = tableIter[i].numTuples();
     tableIter[i].first();

     // first tuple does not match where clause
     //if(tableIter[i].numTuples() == beforeReset){ 
     // boolean firstMatch = tableIter[i].next();

     // table iterator has no matches
     //if(!firstMatch)return false; 
     //}
  
     // return false if the first column reset
     if(i==0) return false;
   }
   if(this.where.isTrue())break;
 }


 numTuples++;
 return true;
    
    }
    
    /**
     * Gets the column at the specified index in the relation that
     * this iterator iterates over.  The leftmost column has an index of 0.
     *
     * @return  the column
     * @throws  IndexOutOfBoundsException if the specified index is invalid
     */
    public Column getColumn(int colIndex) {

 return columns[colIndex];
        /* not yet implemented */
        // return null;
    }
    
    /**
     * Gets the value of the column at the specified index in the row
     * on which this iterator is currently positioned.  The leftmost
     * column has an index of 0.
     *
     * This method will unmarshall the relevant bytes from the
     * key/data pair and return the corresponding Object -- i.e.,
     * an object of type String for CHAR and VARCHAR values, an object
     * of type Integer for INTEGER values, or an object of type Double
     * for REAL values.
     *
     * @return  the value of the column
     * @throws IllegalStateException if the iterator has not yet been
     *         been positioned on a tuple using first() or next()
     * @throws  IndexOutOfBoundsException if the specified index is invalid
     */
    public Object getColumnVal(int colIndex) {
        /* not yet implemented */
        //return null;
     return columns[colIndex].getValue();
    }
    
    public int numColumns() {
        return this.columns.length;
    }
    
    public int numTuples() {
        return this.numTuples;
    }
}
