/*
 * SelectStatement.java
 *
 * DBMS Implementation
 */

import java.util.*;
import com.sleepycat.db.*;

/**
 * A class that represents a SELECT statement.
 */
public class SelectStatement extends SQLStatement {
    /* Used in the selectList for SELECT * statements. */
    public static final String STAR = "*";
    
    private ArrayList<Object> selectList;
    private Limit limit;
    private boolean distinctSpecified;
    
    /** 
     * Constructs a SelectStatement object involving the specified
     * columns and other objects from the SELECT clause, the specified
     * tables from the FROM clause, the specified conditional
     * expression from the WHERE clause (if any), the specified Limit
     * object summarizing the LIMIT clause (if any), and the specified
     * value indicating whether or not we should eliminate duplicates.
     *
     * @param  selectList  the columns and other objects from the SELECT clause
     * @param  fromList  the list of tables from the FROM clause
     * @param  where  the conditional expression from the WHERE clause (if any)
     * @param  limit  summarizes the info in the LIMIT clause (if any)
     * @param  distinctSpecified  should duplicates be eliminated?
     */
    public SelectStatement(ArrayList<Object> selectList, 
                           ArrayList<Table> fromList, ConditionalExpression where,
                           Limit limit, Boolean distinctSpecified)
    {
        super(fromList, new ArrayList<Column>(), where);
        this.selectList = selectList;
        this.limit = limit;
        this.distinctSpecified = distinctSpecified.booleanValue();
        
        /* add the columns in the select list to the list of columns */
        for (int i = 0; i < selectList.size(); i++) {
            Object selectItem = selectList.get(i);
            if (selectItem instanceof Column)
                this.addColumn((Column)selectItem);
        }
    }
    
    /**
     * Returns a boolean value indicating whether duplicates should be
     * eliminated in the result of this statement -- i.e., whether the
     * user specified SELECT DISTINCT.
     */
    public boolean distinctSpecified() {
        return this.distinctSpecified;
    }
    
    public void execute() throws DatabaseException, DeadlockException {
        RelationIterator iter = null;
        
        try {

 
            if (this.numTables() == 1) {
                Table table = this.getTable(0);
                if (table.open() == OperationStatus.NOTFOUND)
                    throw new IllegalStateException();  // error msg in open()
                
                if (this.numColumns() == 0 && this.selectList.get(0) == STAR)
                    iter = new TableIterator(this, table, true);
                else{
      TableIterator sub = new TableIterator(this, table, true);
   //System.out.println("make pi iterator");
      iter = new ProjectionIterator(this, sub);
  }
                   // throw new IllegalStateException("Can't yet handle " +
                                                    //"projections");

            } else{
                //throw new IllegalStateException("Can't yet handle " + 
                //  "multiple tables in the FROM clause");
     
  

  for(int i=0; i<this.numTables(); i++){
   Table table = this.getTable(i);

                 if (table.open() == OperationStatus.NOTFOUND)
                  throw new IllegalStateException();  // error msg in open()
                
                 if (this.numColumns() == 0 && this.selectList.get(0) == STAR){
                      //iter = new TableIterator(this, table, true);
                 }else{
                      //throw new IllegalStateException("Can't yet handle " +
                                //                    "projections");
   }

  }

  iter = new CrossIterator(this);

  if (this.numColumns() != 0 || this.selectList.get(0) != STAR){
                 iter = new ProjectionIterator(this, iter); 
                }
  

     }

  

  ArrayList<String> colNames = new ArrayList<String>();

  for(int i=0; i<this.numTables(); i++){
   Table table = this.getTable(i);
   for(int j=0; j<table.numColumns(); j++){
    colNames.add(table.getColumn(j).getName());
   }
  }

  
  for(int i=0; i<this.numWhereColumns(); i++){
   String name = this.getWhereColumn(i).getName();
   if(!colNames.contains(name)){
    throw new IllegalStateException("Column doesn't exist: " + name);
   }
  
  }


  for(int i=0; i<this.numColumns(); i++){
   String name = this.getColumn(i).getName();
   if(!colNames.contains(name)){
    throw new IllegalStateException("Column doesn't exist: " + name);
   }
  
  }

 
  

            
            // Iterate over all tuples in the relation and print them out.
            iter.printAll(System.out);
            
            // Report the number of tuples that were selected.
            int numRows = iter.numTuples();
            System.out.println("Selected " + numRows + 
                               (numRows == 1 ? " tuple." : " tuples."));
        } catch (Exception e) {
            String errMsg = e.getMessage();
            if (errMsg != null)
                System.err.println(errMsg + ".");
        }
        
        if (iter != null)
            iter.close();
    }
}
