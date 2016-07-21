/*
 * SelectStatement.java
 *
 * DBMS Implementation
 */

import java.util.*;
import com.sleepycat.db.*;
import java.io.*;

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
        /* not yet implemented */

      // table.open()
     
      /*
      try {
            // Close the table's database and remove the table from
            // the in-memory table cache if necessary.
            table.close();
            
            // Remove the table's information from the catalog.
            if (Catalog.removeMetadata(table) == OperationStatus.NOTFOUND)
                throw new Exception(table + ": no such table");
            
            // Remove the underlying database file.
            DBMS.getEnv().removeDatabase(null, table.dbName(), null);
            
            System.out.println("Dropped table " + table + ".");
        } catch (Exception e) {
            String errMsg = e.getMessage();
            if (errMsg != null)
                System.err.println(errMsg + ".");
            System.err.println("Could not drop table " + table + ".");
        }*/
      
     Table table = this.getTable(0);
     try{
     
     if (table.open() != OperationStatus.SUCCESS)
          throw new DatabaseException("Table does not exist.");  // error msg was printed in open()
     
     if (this.numTables() != 1){
       throw new DatabaseException("Only one table allowed in the from clause.");
     }
     
     if (this.numColumns() !=0){
       throw new DatabaseException("Columns cannot be specified.");
     }
     
     } catch (Exception e) {
            String errMsg = e.getMessage();
            if (errMsg != null)
                System.err.println(errMsg);
            System.err.println("Could not proceed with select statement.");
            return;
     }
     
     //if(selectList != null){
     //  throw new DatabaseException("Columns selected: currently not allowed");
     //}
            
    // TableIterator . printAll()
     TableIterator ti = new TableIterator(this, table, false);
     
     ti.printAll(System.out);
     
     int printed = ti.numTuples();
     
     ti.close();
     
     System.out.println("Total Tuples: " + printed);
     
     // print message with number of tuples selected
    
    }
}





