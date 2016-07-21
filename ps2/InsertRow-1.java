/*
 * InsertRow.java
 *
 * DBMS Implementation
 */

import com.sleepycat.db.*;
import com.sleepycat.bind.*;
import com.sleepycat.bind.tuple.*;

import java.util.*;


import java.io.*;

/**
 * A class that represents a row that will be inserted in a table in a
 * relational database.
 *
 * This class contains the code used to marshall the values of the
 * individual columns to a single key-data pair in the underlying
 * BDB database.
 */
public class InsertRow {
    private Table table;         // the table in which the row will be inserted
    private Object[] values;     // the individual values to be inserted
    private DatabaseEntry key;   // the key portion of the marshalled row
    private DatabaseEntry data;  // the data portion of the marshalled row
   
    /**
     * Constructs an InsertRow object for a row containing the specified
     * values that is to be inserted in the specified table.
     *
     * @param  t  the table
     * @param  values  the values in the row to be inserted
     */
    public InsertRow(Table table, Object[] values) {
        this.table = table;
        this.values = values;
        
        // These objects will be created by the marshall() method.
        this.key = null;
        this.data = null;
    }
    
    /**
     * Takes the collection of values for this InsertRow
     * and marshalls them into a key/data pair.
     */
    public void marshall(){
        /* not yet implemented */
        
      ArrayList<Integer> offsets = new ArrayList<Integer>();
      
      int count = table.numColumns();
      
      // get column number of primary key
      int key = -1;
      Column kc = table.primaryKeyColumn();
      if(kc != null){
        key = kc.getIndex();
      }
      
      // add initial offset
      offsets.add(4 * (count + 1));
      
      
      for(int i=0; i<count; i++){
        Column c = table.getColumn(i);
        int type = c.getType();
        
         int previous = offsets.get(offsets.size() - 1);
         
         // if the previous offset was a key, backtrack further
         if(previous == -2){
           previous = offsets.get(offsets.size() - 2);
         }
         
         if(type != 3){
           // use standard length
           offsets.add(previous + c.getLength());
         }else{
           // if the value is a VARCHAR, determine length or null
           if(values[i] == null){
             offsets.add(previous);
           }else{
             offsets.add(previous + ((String)values[i]).length());
           } 
         }
         
         // if column is a key, change the offset to -2
         if(i == key){
           offsets.remove(offsets.size()-1);
           offsets.add(-2);
         }
         
         // if the value is null, set same offset value as previous
         if(values[i] == null){
           offsets.remove(offsets.size()-1);
           
           int prevOS = offsets.get(offsets.size() -1);
           if(prevOS == -2) prevOS = offsets.get(offsets.size() -2);
           
           offsets.add(prevOS);
         }
      }
      
      // get buffer size with offsets, and avoid the primary key -2 value
      int lastOffset = offsets.get(offsets.size() - 1);
      int bufferSize = lastOffset == -2 ? offsets.get(offsets.size()-2): lastOffset;
      
      byte[] buffer = new byte[bufferSize];
      
      TupleOutput t = new TupleOutput(buffer);
      
      // write the offsets
      for(int i=0; i<offsets.size(); i++){
        t.writeInt(offsets.get(i));
      }
      
      // write the values but skip the primary key and nulls
      for(int i=0; i<count; i++){
        if(i == key) continue;
        if(values[i] == null) continue;
        
        int type = table.getColumn(i).getType();
        
        if(type == 0){
         t.writeInt((Integer)values[i]); 
          
        }else if(type ==1){
          t.writeDouble((Double)values[i]);
           
        }else if(type ==2){
          t.writeBytes((String)values[i]);
           
        }else if(type ==3){
          t.writeBytes((String)values[i]);
        }
        
      }
     
      
      
      this.data = new DatabaseEntry(buffer);
      
      
      // if the primary key exist, make an entry for it
      
      if(table.primaryKeyColumn() != null){
      
        Column keyCol = table.getColumn(key);
      
        byte[] kBuffer = new byte[keyCol.getLength() + 4];
      
        TupleOutput kTup = new TupleOutput(kBuffer);
      
        kTup.writeInt(keyCol.getLength());
      
        int kType = keyCol.getType();
        
        if(kType == 0){
          kTup.writeInt((Integer)values[key]); 
          
        }else if(kType ==1){
          kTup.writeDouble((Double)values[key]);
           
        }else if(kType ==2){
          kTup.writeBytes((String)values[key]);
           
        }else if(kType ==3){
          kTup.writeBytes((String)values[key]);
        }
      
        this.key = new DatabaseEntry(kBuffer);
      }
      
      
    }
    
    /**
     * Returns the DatabaseEntry for the key in the key/data pair for this row.
     *
     * @return  the key DatabaseEntry
     */
    public DatabaseEntry getKey() {
        return this.key;
    }
    
    /**
     * Returns the DatabaseEntry for the data item in the key/data pair 
     * for this row.
     *
     * @return  the data DatabaseEntry
     */
    public DatabaseEntry getData() {
        return this.data;
    }
}
