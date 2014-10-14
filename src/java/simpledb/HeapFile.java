package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

	private File file;
	private TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
    	return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {   
    	int offset = pid.pageNumber() * BufferPool.getPageSize();
    	
    	//validate the page read attempt.
    	
    	if (file.length() == 0){
    		if(offset != 0){
    			throw new IllegalArgumentException("Trying to read more content than page has.");
    		}
    	}
    	else if (offset >= file.length() ) {
			throw new IllegalArgumentException("Trying to read more content than page has.");
    	}
    	
    	// Read file page.
        byte[] data = new byte[BufferPool.getPageSize()];    	
    	try {
			RandomAccessFile raf = new RandomAccessFile(file, "r");
			raf.seek(offset);
			raf.read(data);
			raf.close();
			HeapPageId heapPageId = (HeapPageId) pid;
			return new HeapPage(heapPageId, data);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}    	
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
    	int offset = page.getId().pageNumber() * BufferPool.PAGE_SIZE;
    	// write
        byte[] data = page.getPageData();
    	try {
			RandomAccessFile raf = new RandomAccessFile(file, "rw");
			raf.seek(offset);
			raf.write(data);
			raf.close();			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
    	if (file.length() == 0){
    		return 1;
    	}
        return (int) Math.ceil(file.length() / BufferPool.getPageSize()); 
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> pagesAffected = new ArrayList<Page>();
        
        int numPages = numPages();
        for (int i=0; i<numPages; i++){
        	PageId pid = new HeapPageId(getId(), i);
        	HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        	if (heapPage.getNumEmptySlots() != 0){
        		heapPage.insertTuple(t);
        		pagesAffected.add(heapPage);
        		return pagesAffected;
        	}
        }
        // run out of pages in HeapFile.
        // so add a new Page.
        byte[] emptyData = new byte[BufferPool.getPageSize()];
        HeapPageId heapPageId = new HeapPageId(getId(), numPages());
        HeapPage newPage = new HeapPage(heapPageId, emptyData);
        writePage(newPage);
        
        // insert tuple to this latest page.
        PageId pid = new HeapPageId(getId(), numPages);
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        heapPage.insertTuple(t);
        pagesAffected.add(heapPage);
        return pagesAffected;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        ArrayList<Page> pagesAffected = new ArrayList<Page>();
        RecordId rid = t.getRecordId();
        PageId pageId = rid.getPageId();
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        heapPage.deleteTuple(t);
        pagesAffected.add(heapPage);
        return pagesAffected;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
    	return new FileTuplesIterator(tid, getId(), numPages());
    }
    
    private class FileTuplesIterator implements DbFileIterator{

    	private final TransactionId tid;
    	private final int tableId;
    	private final int numPages;
    	private int currentPage;
        private Iterator<Tuple> currentIterator;
    	
    	public FileTuplesIterator(TransactionId tid, int tableId, int numPages){
    		this.tid = tid;
    		this.tableId = tableId;
    		this.numPages = numPages;
    		currentIterator = null;
    	}
    	
    	public Iterator<Tuple> getHeapPageIterator(int pageNo) throws TransactionAbortedException, DbException{
    		HeapPageId pid = new HeapPageId(tableId, pageNo);
    		HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
    		return page.iterator();
    	}
    	public Iterator<Tuple> getCurrentHeapPageIterator() throws TransactionAbortedException, DbException{
    		return getHeapPageIterator(currentPage);
    	}
    	
		@Override
		public void open() throws DbException, TransactionAbortedException {
			currentPage = 0;
			currentIterator = getCurrentHeapPageIterator();
		}

		@Override
		public boolean hasNext() throws DbException,
				TransactionAbortedException {
			if (currentIterator == null){
				return false;
			}
			int pageNo = currentPage;
			Iterator<Tuple> iterator = currentIterator;
			while(!iterator.hasNext()){
				pageNo++;
				if (pageNo >= numPages){
					return false;
				}
				iterator = getHeapPageIterator(pageNo);
			}
			return true;
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException,
				NoSuchElementException {
			if (!this.hasNext()){
				throw new NoSuchElementException("Next element does not exist.");
			}
			while(!currentIterator.hasNext()){
				currentPage++;
				currentIterator = getCurrentHeapPageIterator();
			}
			return currentIterator.next();
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			close();
			open();
		}

		@Override
		public void close() {
			currentIterator = null;
			currentPage = -1;
		}
    	
    }

}

