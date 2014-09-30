package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private final TransactionId tid;
    private DbIterator child;
    private boolean calledFetchNext;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.tid = t;
        this.child = child;
        calledFetchNext = false;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (calledFetchNext){
        	return null;
        }
    	calledFetchNext = true;
    	int numDeletedTuples = 0;
    	while (child.hasNext()){
        	Tuple tuple = child.next();
        	try {
				Database.getBufferPool().deleteTuple(tid, tuple);
				numDeletedTuples++;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
    	int[] tupleData = new int[]{numDeletedTuples};
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
