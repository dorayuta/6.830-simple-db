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
    	int offset = pid.pageNumber() * BufferPool.PAGE_SIZE;
    	//validate the page read attempt.
    	if (file.length() <= offset){
    		throw new IllegalArgumentException("Page does not exist.");
    	}
    	// Read file page.
        byte[] data = new byte[BufferPool.PAGE_SIZE];    	
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
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.ceil(file.length() / BufferPool.PAGE_SIZE); 
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
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

