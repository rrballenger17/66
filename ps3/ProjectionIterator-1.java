/*
 * ProjectionIterator.java
 *
 * DBMS Implementation
 */

import com.sleepycat.db.*;

import java.util.*;

/**
 * A class that serves as an iterator over some or all of the tuples
 * in a relation that results from performing a projection on an 
 * another relation.  It is used to implement all SELECT statements 
 * except ones that specify SELECT *
 */
public class ProjectionIterator extends RelationIterator {
    private Column[] columns;
    private RelationIterator subrel;
    private boolean checkDistinct;
    private int numTuples;
    
    /**
     * Constructs a ProjectionIterator object for the relation formed by
     * applying a projection to the relation over which the specified
     * subrel iterator will iterate.  This other relation is referred to
     * as the "subrelation" of this iterator.
     *
     * @param  stmt    the SQL SELECT statement that specifies the projection
     * @param  subrel  the subrelation
     * @throws IllegalStateException if subrel is null
     */
	private Set<String> tuples;

    public ProjectionIterator(SelectStatement stmt, RelationIterator subrel) {
        /* not yet implemented */

	tuples = new HashSet<String>();

	//System.out.println("HEREERE");	

	this.subrel = subrel;

	columns = new Column[stmt.numColumns()];
	for(int i=0; i<stmt.numColumns(); i++){
		columns[i] = stmt.getColumn(i);
		//System.out.println(columns[i].getName());
		
	}

	checkDistinct = stmt.distinctSpecified();

	numTuples = 0;


    }
    
    /**
     * Closes the iterator, which closes any BDB handles that it is using.
     *
     * @throws DatabaseException if Berkeley DB encounters a problem
     *         while closing a handle
     */
    public void close() throws DatabaseException {
        /* not yet implemented */

	subrel.close();
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
    public boolean next() throws DatabaseException, DeadlockException {
        /* not yet implemented */
	
	
	boolean result = subrel.next();

	while(checkDistinct){
		String concat = "";

		for(int i=0; i<this.numColumns(); i++){
			concat += ("" + this.getColumnVal(i));
		}

		int size = tuples.size();
		tuples.add(concat);

		// new distinct tuple reached
		if(size != tuples.size()) break;
		
		result = subrel.next();
	
		// no more left
		if(!result) break;
	}



	if(result) numTuples++;

        return result;
    }
    
    /**
     * Gets the column at the specified index in the relation that
     * this iterator iterates over.  The leftmost column has an index of 0.
     *
     * @return  the column
     * @throws  IndexOutOfBoundsException if the specified index is invalid
     */
    public Column getColumn(int colIndex) {
        /* not yet implemented */
        //return null;

	if(colIndex >= columns.length) throw new IndexOutOfBoundsException();

	return columns[colIndex];

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
