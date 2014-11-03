package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    private static final long DEADLOCK_TIMEOUT = 10000000000L;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    private int numPages;
    private Map<PageId, Page> cache;
    private Map<PageId, Set<TransactionId>> sharedLocksMap;
    private Map<PageId, TransactionId> exclusiveLockMap;
    private Map<TransactionId, Set<PageId>> transactionLocksMap;
    
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
    	this.numPages = numPages;
        cache = new ConcurrentHashMap<PageId, Page>(numPages);
        sharedLocksMap = new ConcurrentHashMap<PageId, Set<TransactionId>>();
        exclusiveLockMap = new ConcurrentHashMap<PageId, TransactionId>();
        transactionLocksMap = new ConcurrentHashMap<TransactionId, Set<PageId>>();
    }
    
    public static int getPageSize() {
    	return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
    	if (perm.permLevel == Permissions.READ_ONLY.permLevel){
    		// acquire shared lock
    		long startTime = System.nanoTime();
    		synchronized(exclusiveLockMap){
    			while (exclusiveLockMap.containsKey(pid) && !exclusiveLockMap.get(pid).equals(tid)){
    				// block
    				// check timeout
    				long elapsedTime = System.nanoTime() - startTime;
    				if (elapsedTime > DEADLOCK_TIMEOUT){
    					throw new TransactionAbortedException();
    				}
    			}
    			if (exclusiveLockMap.containsKey(pid)){
	    			// now nobody has the exclusive lock.
		    		synchronized(sharedLocksMap){
			    		if (!sharedLocksMap.containsKey(pid)){
			    			sharedLocksMap.put(pid, new HashSet<TransactionId>());
			    			sharedLocksMap.get(pid).add(tid);
			    		}
		    		}
    			}
		}
    	} 
    	else if (perm.permLevel == Permissions.READ_WRITE.permLevel){
    		// block until can acquire exclusive lock
    		synchronized(sharedLocksMap){
        		long startTime = System.nanoTime();
    			while (sharedLocksMap.containsKey(pid) && !sharedLocksMap.get(pid).isEmpty()){
    				// if the only shared lock left is owned by the current transaction
    				if (sharedLocksMap.get(pid).size() == 1 && sharedLocksMap.get(pid).contains(tid)){
    					sharedLocksMap.get(pid).remove(tid);
    					break;
    				}
    				// check timeout
    				long elapsedTime = System.nanoTime() - startTime;
    				if (elapsedTime > DEADLOCK_TIMEOUT){
    					throw new TransactionAbortedException();
    				}
    			}
        		startTime = System.nanoTime();
	    		while (true){
	    			synchronized(exclusiveLockMap){
	    				if (exclusiveLockMap.containsKey(pid) && exclusiveLockMap.get(pid).equals(tid)){
	    					break;
	    				}
	    				if (!exclusiveLockMap.containsKey(pid)){
	    					exclusiveLockMap.put(pid, tid);

	    	    			break;
	    				}
	    			}
    				long elapsedTime = System.nanoTime() - startTime;
    				if (elapsedTime > DEADLOCK_TIMEOUT){
    					throw new TransactionAbortedException();
    				}
	    		}
    		}
    	}
		if (!transactionLocksMap.containsKey(tid)){
			transactionLocksMap.put(tid, new HashSet<PageId>());
		}
		transactionLocksMap.get(tid).add(pid);
    	
        if (cache.containsKey(pid)){
        	return cache.get(pid);
        }
        else{
        	if (cache.size() == this.numPages){
        		evictPage();
        	}
        	Page newPage = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
        	cache.put(pid, newPage);
        	return newPage;
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
    	if (sharedLocksMap.containsKey(pid)){
    		sharedLocksMap.get(pid).remove(tid);	
    	}
//    	synchronized(exclusiveLockMap){
	    	if (exclusiveLockMap.containsKey(pid)){
	    		exclusiveLockMap.remove(pid);
	    	}
//    	}
    	// remove pid from list of locks that tid has.
    	synchronized(transactionLocksMap){
    		transactionLocksMap.get(tid).remove(pid);
    	}
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return transactionLocksMap.containsKey(tid) && transactionLocksMap.get(tid).contains(p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {

    	// TODO - don't know what I'm doing here.
    	if (!transactionLocksMap.containsKey(tid)){
    		return;
    	}
    	synchronized (transactionLocksMap){
	    	if (commit){
	    		// flush all dirty pages
	    		for (PageId pid: transactionLocksMap.get(tid)){
	    			if (cache.containsKey(pid) && cache.get(pid).isDirty() != null){
	    				flushPage(pid);
	    			}
	    		}
	    	}
	    	else{
	    		// abort!!!!! no flush, reread
	    		for (PageId pid: transactionLocksMap.get(tid)){
	    			if (cache.containsKey(pid) && cache.get(pid).isDirty() != null){
	    				cache.remove(pid);
	    			}
	    		}
	    	}
	    	
	    	Set<PageId> newPidSet = new HashSet<PageId>();
	    	for (PageId pid: transactionLocksMap.get(tid)){
	    		newPidSet.add(pid);
	    	}
	    	// release all locks
	    	for (PageId pid: newPidSet){
	    		releasePage(tid, pid);
	    	}
    	}
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
    	
    	DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
    	List<Page> dirtiedPages = dbFile.insertTuple(tid, t);
    	
    	for (Page page: dirtiedPages){
    		page.markDirty(true, tid);	
    		cache.put(page.getId(), page);
    	}
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        int tableId = t.getRecordId().getPageId().getTableId();
    	DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
    	List<Page> dirtiedPages = dbFile.deleteTuple(tid, t);
    	for (Page page: dirtiedPages){
    		page.markDirty(true, tid);
    		cache.put(page.getId(), page);
    	}
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (Page page: cache.values()){
        	if (page.isDirty() != null){
        		flushPage(page.getId());
        	}
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
    	// some code goes here
        // only necessary for lab5
    	cache.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
    	Page page = cache.get(pid);
    	file.writePage(page);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**	
     * Discards a page from the buffer pool. Must not evict a dirty page.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
    	Iterator<PageId> pageIdIter = cache.keySet().iterator();
    	PageId pageIdToEvict;
    	Page pageToEvict;
    	boolean allDirty = true;
    	
    	while (pageIdIter.hasNext()){
    		pageIdToEvict = pageIdIter.next();
    		pageToEvict = cache.get(pageIdToEvict);
    		// if the page is dirty, flush it.
    		if (pageToEvict.isDirty() == null){
    			cache.remove(pageIdToEvict);
    			allDirty = false;
    			break;
    		}
    	}
    	if (allDirty){
    		throw new DbException("Unable to evict any non-dirty pages.");
    	}
    }

}
