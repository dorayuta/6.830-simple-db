package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private final TransactionId tid;
    private DbIterator child;
    private final int tableId;
    private TupleDesc tuplesTD;
    private final TupleDesc tableTD;
    private boolean calledFetchNext;
    
    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        this.tid = t;
        this.child = child;
        this.tableId = tableid;
        this.tuplesTD = child.getTupleDesc();
        this.tableTD = Database.getCatalog().getDatabaseFile(tableId).getTupleDesc();
        if (!tDMatch()){
        	throw new DbException("Can't insert tuples into given table. TupleDesc mismatch.");
        }
        this.calledFetchNext = false;
    }
    
    /**
     * Helper function to determine if the TupleDesc of the tuples
     * and the TupleDesc of the table to insert into match.
     * @return
     */
    public boolean tDMatch(){
    	return tuplesTD.equals(tableTD);
    }

    public TupleDesc getTupleDesc() {
        return Utility.getTupleDesc(1);
    }

    public void open() throws DbException, TransactionAbortedException {
    	super.open();
        child.open();
    }

    public void close() {
    	super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (calledFetchNext){
        	return null;
        }
    	calledFetchNext = true;
    	int numInsertedTuples = 0;
    	while (child.hasNext()){
        	Tuple tuple = child.next();
        	try {
				Database.getBufferPool().insertTuple(tid, tableId, tuple);
				numInsertedTuples++;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }    	
    	int[] tupleData = new int[]{numInsertedTuples};
    	Tuple result = Utility.getTuple(tupleData, 1);
    	return result;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        if (children.length != 1){
        	throw new IllegalArgumentException("There must be exactly child.");
        }
        child = children[0];
    }
}
