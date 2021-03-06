package simpledb.systemtest;

import simpledb.systemtest.SystemTestUtil;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import simpledb.*;
import simpledb.BPlusTreeUtility.*;
import simpledb.Predicate.Op;

/**
 * System test for the BPlusTree
 */
public class BPlusTreeTest extends SimpleDbTestBase {
    private final static Random r = new Random();
    
    private static final int POLL_INTERVAL = 100;
    
    /**
	 * Helper method to clean up the syntax of starting a BPlusTreeInserter thread.
	 * The parameters pass through to the BPlusTreeInserter constructor.
	 */
	public BPlusTreeInserter startInserter(BPlusTreeFile bf, int[] tupdata, BlockingQueue<ArrayList<Integer>> insertedTuples) {

		BPlusTreeInserter bi = new BPlusTreeInserter(bf, tupdata, insertedTuples);
		bi.start();
		return bi;
	}
    
	/**
	 * Helper method to clean up the syntax of starting a BPlusTreeDeleter thread.
	 * The parameters pass through to the BPlusTreeDeleter constructor.
	 */
	public BPlusTreeDeleter startDeleter(BPlusTreeFile bf, BlockingQueue<ArrayList<Integer>> insertedTuples) {

		BPlusTreeDeleter bd = new BPlusTreeDeleter(bf, insertedTuples);
		bd.start();
		return bd;
	}
	
	private void waitForInserterThreads(ArrayList<BPlusTreeInserter> insertThreads) 
			throws Exception {
		Thread.sleep(POLL_INTERVAL);
		for(BPlusTreeInserter thread : insertThreads) {
			while(!thread.succeeded() && thread.getError() == null) {
				Thread.sleep(POLL_INTERVAL);
			}
		}
	}
	
	private void waitForDeleterThreads(ArrayList<BPlusTreeDeleter> deleteThreads) 
			throws Exception {
		Thread.sleep(POLL_INTERVAL);
		for(BPlusTreeDeleter thread : deleteThreads) {
			while(!thread.succeeded() && thread.getError() == null) {
				Thread.sleep(POLL_INTERVAL);
			}
		}
	}
	
	private int[] getRandomTupleData() {
		int item1 = r.nextInt(BPlusTreeUtility.MAX_RAND_VALUE);
		int item2 = r.nextInt(BPlusTreeUtility.MAX_RAND_VALUE);
		return new int[]{item1, item2};
	}
	
    @After
    public void tearDown() throws Exception {
	// set the page size back to the default
	BufferPool.setPageSize(BufferPool.PAGE_SIZE);
	Database.reset();
    }

    /** Test that doing lots of inserts and deletes in multiple threads works */
    @Test public void testBigFile() throws Exception {
    	// For this test we will decrease the size of the Buffer Pool pages
    	BufferPool.setPageSize(1024);
    	
    	// This should create a B+ tree with a packed second tier of internal pages
		// and packed third tier of leaf pages
    	System.out.println("Creating large random B+ tree...");
    	ArrayList<ArrayList<Integer>> tuples = new ArrayList<ArrayList<Integer>>();
		BPlusTreeFile bf = BPlusTreeUtility.createRandomBPlusTreeFile(2, 31000,
				null, tuples, 0);
		
		// we will need more room in the buffer pool for this test
		Database.resetBufferPool(500);
    	
    	ArrayBlockingQueue<ArrayList<Integer>> insertedTuples = new ArrayBlockingQueue<ArrayList<Integer>>(100000);
		insertedTuples.addAll(tuples);
		assertEquals(31000, insertedTuples.size());
		int size = insertedTuples.size();
		
		// now insert some random tuples
		System.out.println("Inserting tuples...");
    	ArrayList<BPlusTreeInserter> insertThreads = new ArrayList<BPlusTreeInserter>();
		for(int i = 0; i < 200; i++) {
			BPlusTreeInserter bi = startInserter(bf, getRandomTupleData(), insertedTuples);
			insertThreads.add(bi);
			// The first few inserts will cause pages to split so give them a little
			// more time to avoid too many deadlock situations
			Thread.sleep(r.nextInt(POLL_INTERVAL));
		}
		
		for(int i = 0; i < 800; i++) {
			BPlusTreeInserter bi = startInserter(bf, getRandomTupleData(), insertedTuples);
			insertThreads.add(bi);
		}
		
		// wait for all threads to finish
		waitForInserterThreads(insertThreads);	
		assertTrue(insertedTuples.size() > size);
		
		// now insert and delete tuples at the same time
		System.out.println("Inserting and deleting tuples...");
    	ArrayList<BPlusTreeDeleter> deleteThreads = new ArrayList<BPlusTreeDeleter>();
		for(BPlusTreeInserter thread : insertThreads) {
			thread.rerun(bf, getRandomTupleData(), insertedTuples);
    		BPlusTreeDeleter bd = startDeleter(bf, insertedTuples);
    		deleteThreads.add(bd);
		}
		
		// wait for all threads to finish
		waitForInserterThreads(insertThreads);
		waitForDeleterThreads(deleteThreads);
		int numPages = bf.numPages();
		size = insertedTuples.size();
		
		// now delete a bunch of tuples
		System.out.println("Deleting tuples...");
		for(int i = 0; i < 10; i++) {
	    	for(BPlusTreeDeleter thread : deleteThreads) {
				thread.rerun(bf, insertedTuples);
			}
			
			// wait for all threads to finish
	    	waitForDeleterThreads(deleteThreads);
		}
		assertTrue(insertedTuples.size() < size);
		size = insertedTuples.size();
		
		// now insert a bunch of random tuples again
		System.out.println("Inserting tuples...");
		for(int i = 0; i < 10; i++) {
	    	for(BPlusTreeInserter thread : insertThreads) {
				thread.rerun(bf, getRandomTupleData(), insertedTuples);
			}
		
			// wait for all threads to finish
	    	waitForInserterThreads(insertThreads);
		}
		assertTrue(insertedTuples.size() > size);
		size = insertedTuples.size();
		// we should be reusing the deleted pages
		assertTrue(bf.numPages() < numPages + 20);
		
		// kill all the threads
		insertThreads = null;
		deleteThreads = null;
		
		ArrayList<ArrayList<Integer>> tuplesList = new ArrayList<ArrayList<Integer>>();
		tuplesList.addAll(insertedTuples);
		TransactionId tid = new TransactionId();
		
		// First look for random tuples and make sure we can find them
		System.out.println("Searching for tuples...");
		for(int i = 0; i < 10000; i++) {
			int rand = r.nextInt(insertedTuples.size());
			ArrayList<Integer> tuple = tuplesList.get(rand);
			IntField randKey = new IntField(tuple.get(bf.keyField()));
			IndexPredicate ipred = new IndexPredicate(Op.EQUALS, randKey);
			DbFileIterator it = bf.indexIterator(tid, ipred);
			it.open();
			boolean found = false;
			while(it.hasNext()) {
				Tuple t = it.next();
				if(tuple.equals(SystemTestUtil.tupleToList(t))) {
					found = true;
					break;
				}
			}
			assertTrue(found);
			it.close();
		}
		
		// now make sure all the tuples are in order and we have the right number
		System.out.println("Performing sanity checks...");
    	DbFileIterator it = bf.iterator(tid);
		Field prev = null;
		it.open();
		int count = 0;
		while(it.hasNext()) {
			Tuple t = it.next();
			if(prev != null) {
				assertTrue(t.getField(bf.keyField()).compare(Op.GREATER_THAN_OR_EQ, prev));
			}
			prev = t.getField(bf.keyField());
			count++;
		}
		it.close();
		assertEquals(count, tuplesList.size());
		Database.getBufferPool().transactionComplete(tid);
		
		// set the page size back
		BufferPool.setPageSize(BufferPool.PAGE_SIZE);
		
    }

    /** Make test compatible with older version of ant. */
    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(BPlusTreeTest.class);
    }
}
