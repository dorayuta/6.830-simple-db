package simpledb;

import java.io.*;
import java.util.*;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import simpledb.Predicate.Op;

/**
 * BPlusTreeFile is an implementation of a DbFile that stores a B+ tree.
 * Specifically, it stores a pointer to a root page,
 * a set of internal pages, and a set of leaf pages, which contain a collection of tuples
 * in sorted order. BPlusTreeFile works closely with BPlusTreeLeafPage, BPlusTreeInternalPage,
 * and BPlusTreeRootPtrPage. The format of these pages is described in their constructors.
 * 
 * @see simpledb.BPlusTreeLeafPage#BPlusTreeLeafPage
 * @see simpledb.BPlusTreeInternalPage#BPlusTreeInternalPage
 * @see simpledb.BPlusTreeRootPtrPage#BPlusTreeRootPtrPage
 * @author Becca Taft
 */
public class BPlusTreeFile implements DbFile {

	private final File f;
	private final TupleDesc td;
	private final int tableid ;
	private int keyField;

	/**
	 * Constructs a B+ tree file backed by the specified file.
	 * 
	 * @param f - the file that stores the on-disk backing store for this B+ tree
	 *            file.
	 * @param key - the field which index is keyed on
	 * @param td - the tuple descriptor of tuples in the file
	 */
	public BPlusTreeFile(File f, int key, TupleDesc td) {
		this.f = f;
		this.tableid = f.getAbsoluteFile().hashCode();
		this.keyField = key;
		this.td = td;
	}

	/**
	 * Returns the File backing this BPlusTreeFile on disk.
	 * 
	 * @return the File backing this BPlusTreeFile on disk.
	 */
	public File getFile() {
		return f;
	}

	/**
	 * Returns an ID uniquely identifying this BPlusTreeFile. Implementation note:
	 * you will need to generate this tableid somewhere ensure that each
	 * BPlusTreeFile has a "unique id," and that you always return the same value for
	 * a particular BPlusTreeFile. We suggest hashing the absolute file name of the
	 * file underlying the BPlusTreeFile, i.e. f.getAbsoluteFile().hashCode().
	 * 
	 * @return an ID uniquely identifying this BPlusTreeFile.
	 */
	public int getId() {
		return tableid;
	}

	/**
	 * Returns the TupleDesc of the table stored in this DbFile.
	 * 
	 * @return TupleDesc of this DbFile.
	 */
	public TupleDesc getTupleDesc() {
		return td;
	}

	/**
	 * Read a page from the file on disk. This should not be called directly
	 * but should be called from the BufferPool via getPage()
	 * 
	 * @param pid - the id of the page to read from disk
	 */
	public Page readPage(PageId pid) {
		BPlusTreePageId id = (BPlusTreePageId) pid;
		BufferedInputStream bis = null;

		try {
			bis = new BufferedInputStream(new FileInputStream(f));
			if(id.pgcateg() == BPlusTreePageId.ROOT_PTR) {
				byte pageBuf[] = new byte[BPlusTreeRootPtrPage.getPageSize()];
				int retval = bis.read(pageBuf, 0, BPlusTreeRootPtrPage.getPageSize());
				if (retval == -1) {
					throw new IllegalArgumentException("Read past end of table");
				}
				if (retval < BPlusTreeRootPtrPage.getPageSize()) {
					throw new IllegalArgumentException("Unable to read "
							+ BPlusTreeRootPtrPage.getPageSize() + " bytes from BPlusTreeFile");
				}
				Debug.log(1, "BPlusTreeFile.readPage: read page %d", id.pageNumber());
				BPlusTreeRootPtrPage p = new BPlusTreeRootPtrPage(id, pageBuf);
				return p;
			}
			else {
				byte pageBuf[] = new byte[BufferPool.getPageSize()];
				if (bis.skip(BPlusTreeRootPtrPage.getPageSize() + (id.pageNumber()-1) * BufferPool.getPageSize()) != 
						BPlusTreeRootPtrPage.getPageSize() + (id.pageNumber()-1) * BufferPool.getPageSize()) {
					throw new IllegalArgumentException(
							"Unable to seek to correct place in BPlusTreeFile");
				}
				int retval = bis.read(pageBuf, 0, BufferPool.getPageSize());
				if (retval == -1) {
					throw new IllegalArgumentException("Read past end of table");
				}
				if (retval < BufferPool.getPageSize()) {
					throw new IllegalArgumentException("Unable to read "
							+ BufferPool.getPageSize() + " bytes from BPlusTreeFile");
				}
				Debug.log(1, "BPlusTreeFile.readPage: read page %d", id.pageNumber());
				if(id.pgcateg() == BPlusTreePageId.INTERNAL) {
					BPlusTreeInternalPage p = new BPlusTreeInternalPage(id, pageBuf, keyField);
					return p;
				}
				else if(id.pgcateg() == BPlusTreePageId.LEAF) {
					BPlusTreeLeafPage p = new BPlusTreeLeafPage(id, pageBuf, keyField);
					return p;
				}
				else { // id.pgcateg() == BPlusTreePageId.HEADER
					BPlusTreeHeaderPage p = new BPlusTreeHeaderPage(id, pageBuf);
					return p;
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			// Close the file on success or error
			try {
				if (bis != null)
					bis.close();
			} catch (IOException ioe) {
				// Ignore failures closing the file
			}
		}
	}

	/**
	 * Write a page to disk.  This should not be called directly but should 
	 * be called from the BufferPool when pages are flushed to disk
	 * 
	 * @param page - the page to write to disk
	 */
	public void writePage(Page page) throws IOException {
		BPlusTreePageId id = (BPlusTreePageId) page.getId();
		
		byte[] data = page.getPageData();
		RandomAccessFile rf = new RandomAccessFile(f, "rw");
		if(id.pgcateg() == BPlusTreePageId.ROOT_PTR) {
			rf.write(data);
			rf.close();
		}
		else {
			rf.seek(BPlusTreeRootPtrPage.getPageSize() + (page.getId().pageNumber()-1) * BufferPool.getPageSize());
			rf.write(data);
			rf.close();
		}
	}

	/**
	 * Returns the number of pages in this BPlusTreeFile.
	 */
	public int numPages() {
		// we only ever write full pages
		return (int) ((f.length() - BPlusTreeRootPtrPage.getPageSize())/ BufferPool.getPageSize());
	}

	/**
	 * Returns the index of the field that this B+ tree is keyed on
	 */
	public int keyField() {
		return keyField;
	}

	/**
	 * Recursive function which finds and locks the leaf page in the B+ tree corresponding to
	 * the left-most page possibly containing the key field f. It locks all internal
	 * nodes along the path to the leaf node with READ_ONLY permission, and locks the 
	 * leaf node with permission perm.
	 * 
	 * If f is null, it finds the left-most leaf page -- used for the iterator
	 * 
	 * @param tid - the transaction id
	 * @param f - the field to search for
	 * @param pid - the current page being searched
	 * @param perm - the permissions with which to lock the leaf page
	 * @return the left-most leaf page possibly containing the key field f
	 * 
	 */
	BPlusTreeLeafPage findLeafPage(TransactionId tid, Field f, BPlusTreePageId pid,
			Permissions perm) 
					throws DbException, TransactionAbortedException {
		// base case.
		if (pid.pgcateg() == BPlusTreePageId.LEAF){
			BPlusTreeLeafPage leafPage = (BPlusTreeLeafPage) Database.getBufferPool().getPage(tid, pid, perm);
			return leafPage;
		}
		// must be internal page.
		BPlusTreeInternalPage internalPage = (BPlusTreeInternalPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
		Iterator<BPlusTreeEntry> iter = internalPage.iterator();

		BPlusTreeEntry entry = null;
		// compare field to internal page keys
		while (iter.hasNext()){
			entry = iter.next();
			Field entryKey = entry.getKey();
			// also check if field is null.
			if (f == null ||  f.compare(Op.LESS_THAN_OR_EQ, entryKey)) {
				BPlusTreePageId leftChildPid = entry.getLeftChild();
				return findLeafPage(tid, f, leftChildPid, perm);
			}
		}
		// if range not found, return rightmost child.
		BPlusTreePageId rightChildPid = entry.getRightChild();
		return findLeafPage(tid, f, rightChildPid, perm);
	}

	/**
	 * Split a leaf page to make room for new tuples and recursively split the parent node
	 * as needed. Update all pointers as needed.
	 * 
	 * @param page - the leaf page to split
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param field - the key field of the tuple to be inserted after the split is complete. Necessary to know
	 * which of the two pages to return.
	 * @return the leaf page into which the new tuple should be inserted
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private BPlusTreeLeafPage splitLeafPage(BPlusTreeLeafPage page, TransactionId tid, HashSet<Page> dirtypages, Field field) 
			throws DbException, IOException, TransactionAbortedException {
		// get Parent Page ID
		BPlusTreePageId parentPageId = page.getParentId();
		
		// if parent page is a root ptr page, create new parent page as root of tree.
		if (parentPageId.pgcateg() == BPlusTreePageId.ROOT_PTR){
			// add root_ptr to dirty pages
			BPlusTreeRootPtrPage rootPtr = getRootPtrPage(tid);
			dirtypages.add(rootPtr);
			
			// construct new parent internal page with pageNo, pageId, and data.
			int newRootNum = getEmptyPage(tid, dirtypages);
			BPlusTreePageId newRootPageId = new BPlusTreePageId(tableid, newRootNum, BPlusTreePageId.INTERNAL);
			
			// discard old page in bufferpool, retrieve new page from bufferpool.
			Database.getBufferPool().discardPage(newRootPageId);
			rootPtr.setRootId(newRootPageId);			
			parentPageId = newRootPageId;
		}
		// get parent page from bufferpool.
		BPlusTreeInternalPage parentPage = (BPlusTreeInternalPage) Database.getBufferPool().getPage(tid, parentPageId, Permissions.READ_WRITE);
		dirtypages.add(parentPage);
		
		// split page to new page
		// construct new pageId and data.
		int newPageNo = getEmptyPage(tid, dirtypages);
	    BPlusTreePageId newPid = new BPlusTreePageId(tableid, newPageNo, BPlusTreePageId.LEAF);
	    
	    // discard any old page.
	    Database.getBufferPool().discardPage(newPid); 
//		byte[] newPageData = BPlusTreeLeafPage.createEmptyPageData();
//		
//		// Construct new page as leftPage and set parent.
//		BPlusTreeLeafPage leftPage = new BPlusTreeLeafPage(newPid, newPageData, keyField);
//		leftPage.setParentId(parentPageId);
		
		// reset leftPage with value from bufferpool
	    BPlusTreeLeafPage leftPage = (BPlusTreeLeafPage) Database.getBufferPool().getPage(tid, newPid, Permissions.READ_WRITE);
		dirtypages.add(leftPage);
		dirtypages.add(page);

		//rename original page as rightPage.
		BPlusTreeLeafPage rightPage = page;
		
		// change the siblings
		if (page.getLeftSiblingId() != null){
			BPlusTreePageId leftSibPid = page.getLeftSiblingId();
			BPlusTreeLeafPage leftSibPage = (BPlusTreeLeafPage)Database.getBufferPool().getPage(tid, leftSibPid, Permissions.READ_WRITE);
			dirtypages.add(leftSibPage);
			leftSibPage.setRightSiblingId(leftPage.getId());
		}
		leftPage.setLeftSiblingId(page.getLeftSiblingId());
		leftPage.setRightSiblingId(rightPage.getId());
		rightPage.setLeftSiblingId(leftPage.getId());
		
		// Iterate through all entries in original page and decide whether to move them.
		Iterator<Tuple> iter = page.iterator();
		int numTuplesToMove = page.getNumTuples() / 2;
		int leftPageTupleCount = 0;

		// tuple to insert.
		Tuple toInsert = new Tuple(td);
		toInsert.setField(keyField, field);
		
		// tuple to push up.
		Tuple toPush = null;
		BPlusTreeLeafPage pageWithInsert = null;

		// move entries and also create entry to insert.
		while(iter.hasNext()){
			Tuple currentTuple = iter.next();
			
			// check if field belongs before currentTuple, only if field location hasn't been decided.
			if (pageWithInsert == null && currentTuple.getField(keyField).compare(Op.GREATER_THAN_OR_EQ, field)) {
								
				// check if left page should have new entry.
				if (leftPageTupleCount <= numTuplesToMove){
					pageWithInsert = leftPage;
				}
				// otherwise, right page should have the new entry.
				else {
					pageWithInsert = rightPage;
				}
			}
			
			// check if currentTuple needs to be moved.
			if (leftPageTupleCount < numTuplesToMove){
				page.deleteTuple(currentTuple);
				leftPage.insertTuple(currentTuple);
				leftPageTupleCount++;
			}
			
			// condition for determining entry/tuple to push up.
			else if (leftPageTupleCount == numTuplesToMove){
				toPush = currentTuple;
				leftPageTupleCount++;
			}
		}
		// insert new entry into old page if it never got set.
		if (pageWithInsert == null){
			pageWithInsert = rightPage;
		}		
		
		BPlusTreeEntry entryToInsert = new BPlusTreeEntry(toPush.getField(keyField), leftPage.getId(), rightPage.getId());
		// check if parent needs to be split
		if (parentPage.getNumEmptySlots() == 0){
			parentPage = splitInternalPage(parentPage, tid, dirtypages, toPush.getField(keyField));
		}
		parentPage.insertEntry(entryToInsert);
		updateParentPointers(tid, parentPage, dirtypages); 		
		return pageWithInsert;
	}

	/**
	 * Recursively split internal pages as needed to make room for new entries. Update
	 * all pointers as needed.
	 * 
	 * @param page - the internal page to split
	 * @param tid - transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param field - the key field of the entry to be inserted after the split is complete. Necessary to know
	 * which of the two pages to return.
	 * @return the internal page into which the new entry should be inserted
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private BPlusTreeInternalPage splitInternalPage(BPlusTreeInternalPage page, TransactionId tid, 
			HashSet<Page> dirtypages, Field field) 
					throws DbException, IOException, TransactionAbortedException {
		// get parent Page Id
		BPlusTreePageId parentPageId = page.getParentId();
		
		// if parent page is a root ptr page, create new parent page as root of tree.
		if (parentPageId.pgcateg() == BPlusTreePageId.ROOT_PTR){
			// add root_ptr to dirty pages
			BPlusTreeRootPtrPage rootPtr = getRootPtrPage(tid);
			dirtypages.add(rootPtr);
			
			// construct new parent internal page with pageNo, pageId, and data.
			int newRootNum = getEmptyPage(tid, dirtypages);
			BPlusTreePageId newRootPageId = new BPlusTreePageId(tableid, newRootNum, BPlusTreePageId.INTERNAL);
			
			// discard old page in bufferpool, retrieve new page from bufferpool.
			Database.getBufferPool().discardPage(newRootPageId);
			rootPtr.setRootId(newRootPageId);			
			parentPageId = newRootPageId;
		}
		
		// get parent page from bufferpool.
		BPlusTreeInternalPage parentPage = (BPlusTreeInternalPage) Database.getBufferPool().getPage(tid, parentPageId, Permissions.READ_WRITE);
		dirtypages.add(parentPage);
		
		// construct new page as left page.
		int newPageNo = getEmptyPage(tid, dirtypages);
	    BPlusTreePageId leftPageId = new BPlusTreePageId(tableid, newPageNo, BPlusTreePageId.INTERNAL);
		byte[] newPageData = BPlusTreeInternalPage.createEmptyPageData();
		// discard any old page
		Database.getBufferPool().discardPage(leftPageId);
		BPlusTreeInternalPage leftPage = new BPlusTreeInternalPage(leftPageId, newPageData, keyField);
		
		// re-get left page from bufferpool.
		leftPage = (BPlusTreeInternalPage) Database.getBufferPool().getPage(tid, leftPageId, Permissions.READ_WRITE);
		dirtypages.add(leftPage);
		
		// rename page as rightPage
		BPlusTreeInternalPage rightPage = page;
		dirtypages.add(rightPage);
		
		// Iterate through all entries in original page and decide whether to move them.
		Iterator<BPlusTreeEntry> iter = page.iterator();
		int numEntriesToMove = page.getNumEntries() / 2;
		int leftPageEntryCount = 0;
		
		BPlusTreeEntry toPush = null;
		BPlusTreeInternalPage pageWithInsert = null;

		// move entries and also create entry to insert.
		while(iter.hasNext()){
			BPlusTreeEntry currentEntry = iter.next();
			
			// Determine page to insert new entry.
			if (pageWithInsert == null && currentEntry.getKey().compare(Op.GREATER_THAN_OR_EQ, field)) {
				
				// check if left/new page should have entry
				if (leftPageEntryCount <= numEntriesToMove){
					pageWithInsert = leftPage;
				}
				// old page (right page) should maintain the new entry.
				else {
					pageWithInsert = rightPage;
				}
			}
			
			// check if currentEntry needs to be moved.
			if (leftPageEntryCount < numEntriesToMove){
				rightPage.deleteEntry(currentEntry);
				leftPage.insertEntry(currentEntry);
				leftPageEntryCount++;
			}
			
			// condition for determining entry to push up.
			else if (leftPageEntryCount == numEntriesToMove){
				rightPage.deleteEntry(currentEntry);
				toPush = new BPlusTreeEntry(currentEntry.getKey(), leftPage.getId(), rightPage.getId());
				leftPageEntryCount++;
			}				
		}
		// case that new entry is last entry
		if (pageWithInsert == null){
			pageWithInsert = rightPage;		
		}
				
		// check if parent needs to be split
		if (parentPage.getNumEmptySlots() == 0){
			parentPage = splitInternalPage(parentPage, tid, dirtypages, toPush.getKey());
		}
		parentPage.insertEntry(toPush);			
		
	    updateParentPointers(tid, parentPage, dirtypages); 
	    updateParentPointers(tid, rightPage, dirtypages);
	    updateParentPointers(tid, leftPage, dirtypages);
	
	    return pageWithInsert;
	}
	
	
	/**
	 * Helper function to update the parent pointer of a node.
	 * 
	 * @param tid - transaction id
	 * @param pid - id of the parent node
	 * @param child - id of the child node to be updated with the parent pointer
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private void updateParentPointer(TransactionId tid, BPlusTreePageId pid, BPlusTreePageId child, HashSet<Page> dirtypages) 
			throws DbException, IOException, TransactionAbortedException {

		if(child.pgcateg() == BPlusTreePageId.LEAF) {
			BPlusTreeLeafPage p = null;
			for (Page page : dirtypages) {
				if (child.equals(page.getId())) {
					p = (BPlusTreeLeafPage) page;
					break;
				}
			}
			if (p == null) {
				p = (BPlusTreeLeafPage) Database.getBufferPool().getPage(tid, child, Permissions.READ_ONLY);
			}

			if(!p.getParentId().equals(pid)) {
				p = (BPlusTreeLeafPage) Database.getBufferPool().getPage(tid, child, Permissions.READ_WRITE);
				p.setParentId(pid);
				dirtypages.add(p);
			}
		}
		else { // child.pgcateg() == BPlusTreePageId.INTERNAL
			BPlusTreeInternalPage p = null;
			for (Page page : dirtypages) {
				if (child.equals(page.getId())) {
					p = (BPlusTreeInternalPage) page;
					break;
				}
			}
			if (p == null) {
				p = (BPlusTreeInternalPage) Database.getBufferPool().getPage(tid, child, Permissions.READ_ONLY);
			}

			if(!p.getParentId().equals(pid)) {
				p = (BPlusTreeInternalPage) Database.getBufferPool().getPage(tid, child, Permissions.READ_WRITE);
				p.setParentId(pid);
				dirtypages.add(p);
			}
		}
	}

	/**
	 * Update the parent pointer of every child of the given page
	 * 
	 * @param tid - transaction id
	 * @param page - the parent page
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private void updateParentPointers(TransactionId tid, BPlusTreeInternalPage page, HashSet<Page> dirtypages) 
			throws DbException, IOException, TransactionAbortedException{
		Iterator<BPlusTreeEntry> it = page.iterator();
		BPlusTreePageId pid = page.getId();
		BPlusTreeEntry e = null;
		while(it.hasNext()) {
			e = it.next();
			updateParentPointer(tid, pid, e.getLeftChild(), dirtypages);
		}
		if(e != null) {
			updateParentPointer(tid, pid, e.getRightChild(), dirtypages);
		}
	}

	/**
	 * Insert a tuple into this BPlusTreeFile, keeping the tuples in sorted order. 
	 * May cause pages to split if the page where tuple t belongs is full.
	 * 
	 * @param tid - the transaction id
	 * @param t - the tuple to insert
	 * @return a list of all pages that were dirtied by this operation. Could include
	 * many pages since parent pointers will need to be updated when an internal node splits.
	 */
	public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		HashSet<Page> dirtypages = new HashSet<Page>();

		// get a read lock on the root pointer page and use it to locate the root page
		BPlusTreeRootPtrPage rootPtr = getRootPtrPage(tid);
		BPlusTreePageId rootId = rootPtr.getRootId();

		if(rootId == null) {		
			rootId = new BPlusTreePageId(tableid, numPages(), BPlusTreePageId.LEAF);
			rootPtr = (BPlusTreeRootPtrPage) Database.getBufferPool().getPage(
					tid, BPlusTreeRootPtrPage.getId(tableid), Permissions.READ_WRITE);
			rootPtr.setRootId(rootId);
			dirtypages.add(rootPtr);
		}

		// find and lock the left-most leaf page corresponding to the key field,
		// and split the leaf page if there are no more slots available
		BPlusTreeLeafPage leafPage = findLeafPage(tid, t.getField(keyField), rootId, Permissions.READ_WRITE);
		if(leafPage.getNumEmptySlots() == 0) {
			leafPage = splitLeafPage(leafPage, tid, dirtypages, t.getField(keyField));	
		}

		// insert the tuple into the leaf page
		leafPage.insertTuple(t);
		dirtypages.add(leafPage);

		ArrayList<Page> dirtyPagesArr = new ArrayList<Page>();
		dirtyPagesArr.addAll(dirtypages);
		return dirtyPagesArr;
	}

	/**
	 * Handle the case when a leaf page becomes less than half full due to deletions.
	 * If one of its siblings has extra tuples, redistribute those tuples.
	 * Otherwise merge with one of the siblings. Update pointers as needed.
	 * 
	 * @param tid - the transaction id
	 * @param page - the leaf page which is less than half full
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private void handleMinOccupancyLeafPage(TransactionId tid, BPlusTreeLeafPage page, HashSet<Page> dirtypages) 
			throws DbException, IOException, TransactionAbortedException {
		BPlusTreePageId parentId = page.getParentId();
		BPlusTreePageId leftSiblingId = null;
		BPlusTreePageId rightSiblingId = null;
		BPlusTreeEntry leftEntry = null;
		BPlusTreeEntry rightEntry = null;
		BPlusTreeInternalPage parent = null;

		// find the left and right siblings through the parent so we make sure they have
		// the same parent as the page. Find the entries in the parent corresponding to 
		// the page and siblings
		if(parentId.pgcateg() != BPlusTreePageId.ROOT_PTR) {
			parent = (BPlusTreeInternalPage) Database.getBufferPool().getPage(
					tid, parentId, Permissions.READ_WRITE);
			Iterator<BPlusTreeEntry> ite = parent.iterator();
			while(ite.hasNext()) {
				BPlusTreeEntry e = ite.next();
				if(e.getLeftChild().equals(page.getId())) {
					rightSiblingId = e.getRightChild();
					rightEntry = e;
					break;
				}
				else if(e.getRightChild().equals(page.getId())) {
					leftSiblingId = e.getLeftChild();
					leftEntry = e;
				}
			}
		}

		int maxEmptySlots = page.getNumTuples() - page.getNumTuples()/2; // ceiling
		if(leftSiblingId != null) {
			BPlusTreeLeafPage leftSibling = (BPlusTreeLeafPage) Database.getBufferPool().getPage(
					tid, leftSiblingId, Permissions.READ_WRITE);
			// if the left sibling is at minimum occupancy, merge with it. Otherwise
			// steal some tuples from it
			if(leftSibling.getNumEmptySlots() >= maxEmptySlots) {
				mergeLeafPages(tid, leftSibling, page, parent, leftEntry, dirtypages);
			}
			else {
		        // Move some of the tuples from the left sibling to the page so
				// that the tuples are evenly distributed. Be sure to update
				// the corresponding parent entry.
				page.markDirty(true, tid);
				leftSibling.markDirty(true, tid);
				dirtypages.add(page);
				dirtypages.add(leftSibling);
				distributeTuples(parent, leftSibling, page, true);
			}
			dirtypages.add(leftSibling);
			dirtypages.add(parent);
		}
		else if(rightSiblingId != null) {	
			BPlusTreeLeafPage rightSibling = (BPlusTreeLeafPage) Database.getBufferPool().getPage(
					tid, rightSiblingId, Permissions.READ_WRITE);
			// if the right sibling is at minimum occupancy, merge with it. Otherwise
			// steal some tuples from it
			if(rightSibling.getNumEmptySlots() >= maxEmptySlots) {
				mergeLeafPages(tid, page, rightSibling, parent, rightEntry, dirtypages);
			}
			else {
		        // Move some of the tuples from the right sibling to the page so
				// that the tuples are evenly distributed. Be sure to update
				// the corresponding parent entry.
				page.markDirty(true, tid);
				rightSibling.markDirty(true, tid);
				dirtypages.add(page);
				dirtypages.add(rightSibling);
				distributeTuples(parent, page, rightSibling, false);
			}
			dirtypages.add(rightSibling);
			dirtypages.add(parent);
		}
	}

	/**
	 * Helper to distribute tuples from one leaf page to another.
	 * @param parentPage
	 * @param from
	 * @param to
	 * @throws DbException 
	 */
	private void distributeTuples(BPlusTreeInternalPage parentPage, 
			BPlusTreeLeafPage leftPage, BPlusTreeLeafPage rightPage, boolean leftToRight) throws DbException{
				
		BPlusTreeLeafPage from = leftPage;
		BPlusTreeLeafPage to = rightPage;
		if (!leftToRight){
			from = rightPage;
			to = leftPage;
		}
		
		// move tuples from -> to.
		Iterator<Tuple> fromIter = from.iterator();
		List<Tuple> tupleList = new ArrayList<Tuple>();
		while (fromIter.hasNext()){
			tupleList.add(fromIter.next());
		}
		if (leftToRight){
			Collections.reverse(tupleList);
		}
		for (Tuple tuple: tupleList){
			if (to.getNumEmptySlots() <= from.getNumEmptySlots()){
				break;
			}
			from.deleteTuple(tuple);
			to.insertTuple(tuple);
		}
		
		// new field for updated parent entry
		Field entryUpdateField = rightPage.iterator().next().getField(keyField);
		BPlusTreeEntry newParentEntry = new BPlusTreeEntry(entryUpdateField, leftPage.getId(), rightPage.getId());
		
		BPlusTreeEntry entryToUpdate = null;
		// find parent entry to update.
		Iterator<BPlusTreeEntry> parentEntryIter = parentPage.iterator();
		while (parentEntryIter.hasNext()){
			BPlusTreeEntry currentEntry = parentEntryIter.next();
			if (currentEntry.getLeftChild().equals(leftPage.getId()) && currentEntry.getRightChild().equals(rightPage.getId())){
				entryToUpdate = currentEntry;
			}
		}
		
		parentPage.deleteEntry(entryToUpdate);
		parentPage.insertEntry(newParentEntry);
	}
	
	/**
	 * Handle the case when an internal page becomes less than half full due to deletions.
	 * If one of its siblings has extra entries, redistribute those entries.
	 * Otherwise merge with one of the siblings. Update pointers as needed.
	 * 
	 * @param tid - the transaction id
	 * @param page - the internal page which is less than half full
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private void handleMinOccupancyInternalPage(TransactionId tid, BPlusTreeInternalPage page, 
			HashSet<Page> dirtypages) 
					throws DbException, IOException, TransactionAbortedException {
		BPlusTreePageId parentId = page.getParentId();
		BPlusTreePageId leftSiblingId = null;
		BPlusTreePageId rightSiblingId = null;
		BPlusTreeEntry leftEntry = null;
		BPlusTreeEntry rightEntry = null;
		BPlusTreeInternalPage parent = null;

		// find the left and right siblings through the parent so we make sure they have
		// the same parent as the page. Find the entries in the parent corresponding to 
		// the page and siblings
		if(parentId.pgcateg() != BPlusTreePageId.ROOT_PTR) {
			parent = (BPlusTreeInternalPage) Database.getBufferPool().getPage(
					tid, parentId, Permissions.READ_WRITE);
			Iterator<BPlusTreeEntry> ite = parent.iterator();
			while(ite.hasNext()) {
				BPlusTreeEntry e = ite.next();
				if(e.getLeftChild().equals(page.getId())) {
					rightSiblingId = e.getRightChild();
					rightEntry = e;
					break;
				}
				else if(e.getRightChild().equals(page.getId())) {
					leftSiblingId = e.getLeftChild();
					leftEntry = e;
				}
			}
		}

		int maxEmptySlots = page.getNumEntries() - page.getNumEntries()/2; // ceiling
		if(leftSiblingId != null) {
			BPlusTreeInternalPage leftSibling = (BPlusTreeInternalPage) Database.getBufferPool().getPage(
					tid, leftSiblingId, Permissions.READ_WRITE);
			// if the left sibling is at minimum occupancy, merge with it. Otherwise
			// steal some entries from it
			if(leftSibling.getNumEmptySlots() >= maxEmptySlots) {
				mergeInternalPages(tid, leftSibling, page, parent, leftEntry, dirtypages);
			}
			else {
		        // Move some of the entries from the left sibling to the page so
				// that the entries are evenly distributed. Be sure to update
				// the corresponding parent entry. Be sure to update the parent
				// pointers of all children in the entries that were moved.
				page.markDirty(true, tid);
				leftSibling.markDirty(true, tid);
				dirtypages.add(page);
				dirtypages.add(leftSibling);
				distributeEntries(parent, leftSibling, page, true, leftEntry);
				updateParentPointers(tid, parent, dirtypages);
			}
			dirtypages.add(leftSibling);
			dirtypages.add(parent);
		}
		else if(rightSiblingId != null) {
			BPlusTreeInternalPage rightSibling = (BPlusTreeInternalPage) Database.getBufferPool().getPage(
					tid, rightSiblingId, Permissions.READ_WRITE);
			// if the right sibling is at minimum occupancy, merge with it. Otherwise
			// steal some entries from it
			if(rightSibling.getNumEmptySlots() >= maxEmptySlots) {
				mergeInternalPages(tid, page, rightSibling, parent, rightEntry, dirtypages);
			}
			else {
		        // Move some of the entries from the right sibling to the page so
				// that the entries are evenly distributed. Be sure to update
				// the corresponding parent entry. Be sure to update the parent
				// pointers of all children in the entries that were moved.
				page.markDirty(true, tid);
				rightSibling.markDirty(true, tid);
				dirtypages.add(page);
				dirtypages.add(rightSibling);
				distributeEntries(parent, page, rightSibling, false, rightEntry);
				updateParentPointers(tid, parent, dirtypages);
			}
			dirtypages.add(rightSibling);
			dirtypages.add(parent);
		}
	}
	
	/**
	 * Helper to distribute entries from one internal page to its sibling.
	 * @param parentPage
	 * @param leftPage
	 * @param rightPage
	 * @param leftToRight
	 * @param oldParentEntry
	 * @throws DbException
	 */
	private void distributeEntries(BPlusTreeInternalPage parentPage, 
			BPlusTreeInternalPage leftPage, BPlusTreeInternalPage rightPage, boolean leftToRight, BPlusTreeEntry oldParentEntry) throws DbException{
		
		BPlusTreeInternalPage from = leftPage;
		BPlusTreeInternalPage to = rightPage;
		if (!leftToRight){
			from = rightPage;
			to = leftPage;
		}
		
		// move entries from -> to.
		Iterator<BPlusTreeEntry> fromIter = from.iterator();
		List<BPlusTreeEntry> entryList = new ArrayList<BPlusTreeEntry>();
		while (fromIter.hasNext()){
			entryList.add(fromIter.next());
		}
		if (leftToRight){
			Collections.reverse(entryList);
		}
		// determine right and left child of old parenty entry.
		BPlusTreePageId leftChildId = null;
		BPlusTreePageId rightChildId = null;
		if (leftToRight){
			leftChildId = entryList.get(0).getRightChild();
			rightChildId = to.iterator().next().getLeftChild();
		}
		else{
			// get last entry of "to" page and it's right child.
			Iterator<BPlusTreeEntry> toIter = to.iterator();
			while (toIter.hasNext()){
				leftChildId = toIter.next().getRightChild();
			}
			rightChildId = entryList.get(0).getLeftChild();
		}
		// move old parent entry down to child first.
		to.insertEntry(new BPlusTreeEntry(oldParentEntry.getKey(), leftChildId, rightChildId));
		
		// moving of tuples.
		for (BPlusTreeEntry entry: entryList){
			if (to.getNumEmptySlots() <= from.getNumEmptySlots()){
				break;
			}
			from.deleteEntry(entry);
			to.insertEntry(entry);
		}
		
		// new field for updated parent entry
		Field entryUpdateField = rightPage.iterator().next().getKey();
		BPlusTreeEntry newParentEntry = new BPlusTreeEntry(entryUpdateField, leftPage.getId(), to.getId());
		
		BPlusTreeEntry entryToUpdate = null;
		// find parent entry to udpate.
		Iterator<BPlusTreeEntry> parentEntryIter = parentPage.iterator();
		while (parentEntryIter.hasNext()){
			BPlusTreeEntry currentEntry = parentEntryIter.next();
			if (currentEntry.getLeftChild().equals(leftPage.getId()) && currentEntry.getRightChild().equals(rightPage.getId())){
				entryToUpdate = currentEntry;
			}
		}
		parentPage.deleteEntry(entryToUpdate);
		parentPage.insertEntry(newParentEntry);
	}

	/**
	 * Merge two leaf pages by moving all tuples from the right page to the left page. 
	 * Recursively handle the case when the parent gets below minimum occupancy.
	 * 
	 * @param tid - the transaction id
	 * @param leftPage - the left leaf page
	 * @param rightPage - the right leaf page
	 * @param parent - the parent of the two pages
	 * @param parentEntry - the entry in the parent corresponding to the leftPage and rightPage
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private void mergeLeafPages(TransactionId tid, BPlusTreeLeafPage leftPage, 
			BPlusTreeLeafPage rightPage, BPlusTreeInternalPage parent, BPlusTreeEntry parentEntry, HashSet<Page> dirtypages) 
					throws DbException, IOException, TransactionAbortedException {
		// delete the entry in the parent corresponding to the two pages. If
		// the parent is below minimum occupancy, get some tuples from its siblings
		// or merge with one of the siblings
		parent.deleteEntry(parentEntry);
		int maxEmptySlots = parent.getNumEntries() - parent.getNumEntries()/2; // ceiling
		if(parent.getNumEmptySlots() == parent.getNumEntries()) {
			// This was the last entry in the parent.
			// In this case, the parent (root node) should be deleted, and the merged 
			// page will become the new root
			BPlusTreePageId rootPtrId = parent.getParentId();
			if(rootPtrId.pgcateg() != BPlusTreePageId.ROOT_PTR) {
				throw new DbException("attempting to delete a non-root node");
			}
			BPlusTreeRootPtrPage rootPtr = (BPlusTreeRootPtrPage) Database.getBufferPool().getPage(
					tid, rootPtrId, Permissions.READ_WRITE);
			leftPage.setParentId(rootPtrId);
			rootPtr.setRootId(leftPage.getId());
			dirtypages.add(rootPtr);

			// release the parent page for reuse
			setEmptyPage(tid, dirtypages, parent.getId().pageNumber());
		}
		else if(parent.getNumEmptySlots() > maxEmptySlots) { 
			handleMinOccupancyInternalPage(tid, parent, dirtypages);
		}

		// Move all the tuples from the right page to the left page, update
		// the sibling pointers, and make the right page available for reuse
		Iterator<Tuple> iter = rightPage.iterator();
		while (iter.hasNext()){
			Tuple currentTuple = iter.next();
			rightPage.deleteTuple(currentTuple);
			leftPage.insertTuple(currentTuple);
		}
		// update dirty pages.
		dirtypages.add(parent);
		dirtypages.add(leftPage);
		dirtypages.add(rightPage);
		
		// update right sibling of left page now that right page no longer exists.
		BPlusTreePageId newRightSiblingId = rightPage.getRightSiblingId();
		leftPage.setRightSiblingId(newRightSiblingId);
		
		setEmptyPage(tid, dirtypages, rightPage.getId().pageNumber());
	}

	/**
	 * Merge two internal pages by moving all entries from the right page to the left page. 
	 * Recursively handle the case when the parent gets below minimum occupancy.
	 * 
	 * @param tid - the transaction id
	 * @param leftPage - the left internal page
	 * @param rightPage - the right internal page
	 * @param parent - the parent of the two pages
	 * @param parentEntry - the entry in the parent corresponding to the leftPage and rightPage
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private void mergeInternalPages(TransactionId tid, BPlusTreeInternalPage leftPage, 
			BPlusTreeInternalPage rightPage, BPlusTreeInternalPage parent, BPlusTreeEntry parentEntry, HashSet<Page> dirtypages) 
					throws DbException, IOException, TransactionAbortedException {
		// delete the entry in the parent corresponding to the two pages.  If
		// the parent is below minimum occupancy, get some tuples from its siblings
		// or merge with one of the siblings
		parent.deleteEntry(parentEntry);
		int maxEmptySlots = parent.getNumEntries() - parent.getNumEntries()/2; // ceiling
		if(parent.getNumEmptySlots() == parent.getNumEntries()) {
			// This was the last entry in the parent.
			// In this case, the parent (root node) should be deleted, and the merged 
			// page will become the new root
			BPlusTreePageId rootPtrId = parent.getParentId();
			if(rootPtrId.pgcateg() != BPlusTreePageId.ROOT_PTR) {
				throw new DbException("attempting to delete a non-root node");
			}
			BPlusTreeRootPtrPage rootPtr = (BPlusTreeRootPtrPage) Database.getBufferPool().getPage(
					tid, rootPtrId, Permissions.READ_WRITE);
			leftPage.setParentId(rootPtrId);
			rootPtr.setRootId(leftPage.getId());
			dirtypages.add(rootPtr);

			// release the parent page for reuse
			setEmptyPage(tid, dirtypages, parent.getId().pageNumber());
		}
		else if(parent.getNumEmptySlots() > maxEmptySlots) { 
			handleMinOccupancyInternalPage(tid, parent, dirtypages);
		}

        // Move all the entries from the right page to the left page, update
		// the parent pointers of the children in the entries that were moved, 
		// and make the right page available for reuse
		
		// determine right and left child of old parent entry
		BPlusTreePageId leftChildId = null;
		BPlusTreePageId rightChildId = rightPage.iterator().next().getLeftChild();
		Iterator<BPlusTreeEntry> entryIter = leftPage.iterator();
		while(entryIter.hasNext()){
			leftChildId = entryIter.next().getRightChild();
		}
		
		// insert old parenty entry into left page
		leftPage.insertEntry(new BPlusTreeEntry(parentEntry.getKey(), leftChildId, rightChildId));
		
		// update dirty pages
		dirtypages.add(parent);
		dirtypages.add(leftPage);
		dirtypages.add(rightPage);
		
		// update parent pointers
		updateParentPointers(tid, parent, dirtypages);
		updateParentPointers(tid, leftPage, dirtypages);
		
		// set right page as now empty
		setEmptyPage(tid, dirtypages, rightPage.getId().pageNumber());
		
	}

	/**
	 * Delete a tuple from this BPlusTreeFile. 
	 * May cause pages to merge or redistribute entries/tuples if the pages 
	 * become less than half full.
	 * 
	 * @param tid - the transaction id
	 * @param t - the tuple to delete
	 * @return a list of all pages that were dirtied by this operation. Could include
	 * many pages since parent pointers will need to be updated when an internal node merges.
	 */
	public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) 
			throws DbException, IOException, TransactionAbortedException {
		HashSet<Page> dirtypages = new HashSet<Page>();

		BPlusTreePageId pageId = new BPlusTreePageId(tableid, t.getRecordId().getPageId().pageNumber(), 
				BPlusTreePageId.LEAF);
		BPlusTreeLeafPage page = (BPlusTreeLeafPage) Database.getBufferPool().getPage(
				tid, pageId, Permissions.READ_WRITE);
		page.deleteTuple(t);

		// if the page is below minimum occupancy, get some tuples from its siblings
		// or merge with one of the siblings
		int maxEmptySlots = page.getNumTuples() - page.getNumTuples()/2; // ceiling
		if(page.getNumEmptySlots() > maxEmptySlots) { 
			handleMinOccupancyLeafPage(tid, page, dirtypages);
		}
		dirtypages.add(page);

		ArrayList<Page> dirtyPagesArr = new ArrayList<Page>();
		dirtyPagesArr.addAll(dirtypages);
		return dirtyPagesArr;
	}

	/**
	 * Get a read lock on the root pointer page. Create the root pointer page and root page
	 * if necessary.
	 * 
	 * @param tid - transaction id
	 * @return the root pointer page
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private BPlusTreeRootPtrPage getRootPtrPage(TransactionId tid) throws DbException, IOException, TransactionAbortedException {
		synchronized(this) {
			if(f.length() == 0) {
				// create the root pointer page and the root page
				BufferedOutputStream bw = new BufferedOutputStream(
						new FileOutputStream(f, true));
				byte[] emptyRootPtrData = BPlusTreeRootPtrPage.createEmptyPageData();
				byte[] emptyLeafData = BPlusTreeLeafPage.createEmptyPageData();
				bw.write(emptyRootPtrData);
				bw.write(emptyLeafData);
				bw.close();
			}
		}

		// get a read lock on the root pointer page
		return (BPlusTreeRootPtrPage) Database.getBufferPool().getPage(
				tid, BPlusTreeRootPtrPage.getId(tableid), Permissions.READ_ONLY);
	}

	/**
	 * Get the page number of the first empty page in this BPlusTreeFile.
	 * Creates a new page if none of the existing pages are empty.
	 * 
	 * @param tid - transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @return the page number of the first empty page
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private int getEmptyPage(TransactionId tid, HashSet<Page> dirtypages) 
			throws DbException, IOException, TransactionAbortedException {
		// get a read lock on the root pointer page and use it to locate the first header page
		BPlusTreeRootPtrPage rootPtr = getRootPtrPage(tid);
		BPlusTreePageId headerId = rootPtr.getHeaderId();
		int emptyPageNo = 0;

		if(headerId != null) {
			BPlusTreeHeaderPage headerPage = (BPlusTreeHeaderPage) Database.getBufferPool().getPage(
					tid, headerId, Permissions.READ_ONLY);
			int headerPageCount = 0;
			// try to find a header page with an empty slot
			while(headerPage != null && headerPage.getEmptySlot() == -1) {
				headerId = headerPage.getNextPageId();
				if(headerId != null) {
					headerPage = (BPlusTreeHeaderPage) Database.getBufferPool().getPage(
							tid, headerId, Permissions.READ_ONLY);
					headerPageCount++;
				}
				else {
					headerPage = null;
				}
			}

			// if headerPage is not null, it must have an empty slot
			if(headerPage != null) {
				headerPage = (BPlusTreeHeaderPage) Database.getBufferPool().getPage(
						tid, headerId, Permissions.READ_WRITE);
				int emptySlot = headerPage.getEmptySlot();
				headerPage.markSlotUsed(emptySlot, true);
				emptyPageNo = headerPageCount * BPlusTreeHeaderPage.getNumSlots() + emptySlot;
				// make sure this empty page is not in the BufferPool
				//Database.getBufferPool().discardPage(new HeapPageId(tableid, emptyPageNo));
				dirtypages.add(headerPage);
			}
		}

		// at this point if headerId is null, either there are no header pages 
		// or there are no free slots
		if(headerId == null) {		
			synchronized(this) {
				// create the new page
				BufferedOutputStream bw = new BufferedOutputStream(
						new FileOutputStream(f, true));
				byte[] emptyData = BPlusTreeInternalPage.createEmptyPageData();
				bw.write(emptyData);
				bw.close();
				emptyPageNo = numPages();
			}
		}

		return emptyPageNo; 
	}

	/**
	 * Mark a page in this BPlusTreeFile as empty. 
	 * If this is the last page in the file, just truncate the file. Otherwise, 
	 * find the corresponding header page (create it if needed), and mark
	 * the corresponding slot in the header page as empty.
	 * 
	 * @param tid - transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param emptyPageNo - the page number of the empty page
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private void setEmptyPage(TransactionId tid, HashSet<Page> dirtypages, int emptyPageNo) 
			throws DbException, IOException, TransactionAbortedException {

		// if this is the last page in the file (and not the only page), just 
		// truncate the file
		synchronized(this) {
			if(emptyPageNo == numPages()) {
				if(emptyPageNo <= 1) {
					// if this is the only page in the file, just return.
					// It just means we have an empty root page
					return;
				}
				long newSize = f.length() - BufferPool.getPageSize();
				FileOutputStream fos = new FileOutputStream(f, true);
				FileChannel fc = fos.getChannel();
				fc.truncate(newSize);
				fc.close();
				fos.close();
				return;
			}
		}

		// otherwise, get a read lock on the root pointer page and use it to locate 
		// the first header page
		BPlusTreeRootPtrPage rootPtr = getRootPtrPage(tid);
		BPlusTreePageId headerId = rootPtr.getHeaderId();
		BPlusTreePageId prevId = null;
		int headerPageCount = 0;

		// if there are no header pages, create the first header page and update
		// the header pointer in the BPlusTreeRootPtrPage
		if(headerId == null) {
			rootPtr = (BPlusTreeRootPtrPage) Database.getBufferPool().getPage(
					tid, BPlusTreeRootPtrPage.getId(tableid), Permissions.READ_WRITE);
			int headerPageNo = getEmptyPage(tid, dirtypages);
			headerId = new BPlusTreePageId(tableid, headerPageNo, BPlusTreePageId.HEADER);
			writePage(new BPlusTreeHeaderPage(headerId, BPlusTreeHeaderPage.createEmptyPageData()));
			// make sure the page is not in the buffer pool
			Database.getBufferPool().discardPage(headerId);
			BPlusTreeHeaderPage headerPage = (BPlusTreeHeaderPage) Database.getBufferPool().getPage(
					tid, headerId, Permissions.READ_WRITE);
			headerPage.init();
			rootPtr.setHeaderId(headerId);

			dirtypages.add(headerPage);
			dirtypages.add(rootPtr);
		}

		// iterate through all the existing header pages to find the one containing the slot
		// corresponding to emptyPageNo
		while(headerId != null && (headerPageCount + 1) * BPlusTreeHeaderPage.getNumSlots() < emptyPageNo) {
			BPlusTreeHeaderPage headerPage = (BPlusTreeHeaderPage) Database.getBufferPool().getPage(
					tid, headerId, Permissions.READ_ONLY);
			prevId = headerId;
			headerId = headerPage.getNextPageId();
			headerPageCount++;
		}

		// at this point headerId should either be null or set with 
		// the headerPage containing the slot corresponding to emptyPageNo.
		// Add header pages until we have one with a slot corresponding to emptyPageNo
		while((headerPageCount + 1) * BPlusTreeHeaderPage.getNumSlots() < emptyPageNo) {
			BPlusTreeHeaderPage prevPage = (BPlusTreeHeaderPage) Database.getBufferPool().getPage(
					tid, prevId, Permissions.READ_WRITE);
			int headerPageNo = getEmptyPage(tid, dirtypages);
			headerId = new BPlusTreePageId(tableid, headerPageNo, BPlusTreePageId.HEADER);
			writePage(new BPlusTreeHeaderPage(headerId, BPlusTreeHeaderPage.createEmptyPageData()));
			// make sure the page is not in the buffer pool			
			Database.getBufferPool().discardPage(headerId);
			BPlusTreeHeaderPage headerPage = (BPlusTreeHeaderPage) Database.getBufferPool().getPage(
					tid, headerId, Permissions.READ_WRITE);
			headerPage.init();
			headerPage.setPrevPageId(prevId);
			prevPage.setNextPageId(headerId);
			prevId = headerId;

			dirtypages.add(headerPage);
			dirtypages.add(prevPage);
			headerPageCount++;
		}

		// now headerId should be set with the headerPage containing the slot corresponding to 
		// emptyPageNo
		BPlusTreeHeaderPage headerPage = (BPlusTreeHeaderPage) Database.getBufferPool().getPage(
				tid, headerId, Permissions.READ_WRITE);
		int emptySlot = emptyPageNo - headerPageCount * BPlusTreeHeaderPage.getNumSlots();
		headerPage.markSlotUsed(emptySlot, false);
		dirtypages.add(headerPage);
	}

	/**
	 * get the specified tuples from the file based on its IndexPredicate value on
	 * behalf of the specified transaction. This method will acquire a read lock on
	 * the affected pages of the file, and may block until the lock can be
	 * acquired.
	 * 
	 * @param tid - the transaction id
	 * @param ipred - the index predicate value to filter on
	 * @return an iterator for the filtered tuples
	 */
	public DbFileIterator indexIterator(TransactionId tid, IndexPredicate ipred) {
		return new BPlusTreeSearchIterator(this, tid, ipred);
	}

	/**
	 * Get an iterator for all tuples in this B+ tree file in sorted order. This method 
	 * will acquire a read lock on the affected pages of the file, and may block until 
	 * the lock can be acquired.
	 * 
	 * @param tid - the transaction id
	 * @return an iterator for all the tuples in this file
	 */
	public DbFileIterator iterator(TransactionId tid) {
		return new BPlusTreeFileIterator(this, tid);
	}

}

/**
 * Helper class that implements the Java Iterator for tuples on a BPlusTreeFile
 */
class BPlusTreeFileIterator extends AbstractDbFileIterator {

	Iterator<Tuple> it = null;
	BPlusTreeLeafPage curp = null;

	TransactionId tid;
	BPlusTreeFile f;

	/**
	 * Constructor for this iterator
	 * @param f - the BPlusTreeFile containing the tuples
	 * @param tid - the transaction id
	 */
	public BPlusTreeFileIterator(BPlusTreeFile f, TransactionId tid) {
		this.f = f;
		this.tid = tid;
	}

	/**
	 * Open this iterator by getting an iterator on the first leaf page
	 */
	public void open() throws DbException, TransactionAbortedException {
		BPlusTreeRootPtrPage rootPtr = (BPlusTreeRootPtrPage) Database.getBufferPool().getPage(
				tid, BPlusTreeRootPtrPage.getId(f.getId()), Permissions.READ_ONLY);
		BPlusTreePageId root = rootPtr.getRootId();
		curp = f.findLeafPage(tid, null, root, Permissions.READ_ONLY);
		it = curp.iterator();
	}

	/**
	 * Read the next tuple either from the current page if it has more tuples or
	 * from the next page by following the right sibling pointer.
	 * 
	 * @return the next tuple, or null if none exists
	 */
	@Override
	protected Tuple readNext() throws TransactionAbortedException, DbException {
		if (it != null && !it.hasNext())
			it = null;

		while (it == null && curp != null) {
			BPlusTreePageId nextp = curp.getRightSiblingId();
			if(nextp == null) {
				curp = null;
			}
			else {
				curp = (BPlusTreeLeafPage) Database.getBufferPool().getPage(tid,
						nextp, Permissions.READ_ONLY);
				it = curp.iterator();
				if (!it.hasNext())
					it = null;
			}
		}

		if (it == null)
			return null;
		return it.next();
	}

	/**
	 * rewind this iterator back to the beginning of the tuples
	 */
	public void rewind() throws DbException, TransactionAbortedException {
		close();
		open();
	}

	/**
	 * close the iterator
	 */
	public void close() {
		super.close();
		it = null;
		curp = null;
	}
}

/**
 * Helper class that implements the DbFileIterator for search tuples on a
 * B+ Tree File
 */
class BPlusTreeSearchIterator extends AbstractDbFileIterator {

	Iterator<Tuple> it = null;
	BPlusTreeLeafPage curp = null;

	TransactionId tid;
	BPlusTreeFile f;
	IndexPredicate ipred;

	/**
	 * Constructor for this iterator
	 * @param f - the BPlusTreeFile containing the tuples
	 * @param tid - the transaction id
	 * @param ipred - the predicate to filter on
	 */
	public BPlusTreeSearchIterator(BPlusTreeFile f, TransactionId tid, IndexPredicate ipred) {
		this.f = f;
		this.tid = tid;
		this.ipred = ipred;
	}

	/**
	 * Open this iterator by getting an iterator on the first leaf page applicable
	 * for the given predicate operation
	 */
	public void open() throws DbException, TransactionAbortedException {
		BPlusTreeRootPtrPage rootPtr = (BPlusTreeRootPtrPage) Database.getBufferPool().getPage(
				tid, BPlusTreeRootPtrPage.getId(f.getId()), Permissions.READ_ONLY);
		BPlusTreePageId root = rootPtr.getRootId();
		if(ipred.getOp() == Op.EQUALS || ipred.getOp() == Op.GREATER_THAN 
				|| ipred.getOp() == Op.GREATER_THAN_OR_EQ) {
			curp = f.findLeafPage(tid, ipred.getField(), root, Permissions.READ_ONLY);
		}
		else {
			curp = f.findLeafPage(tid, null, root, Permissions.READ_ONLY);
		}
		it = curp.iterator();
	}

	/**
	 * Read the next tuple either from the current page if it has more tuples matching
	 * the predicate or from the next page by following the right sibling pointer.
	 * 
	 * @return the next tuple matching the predicate, or null if none exists
	 */
	@Override
	protected Tuple readNext() throws TransactionAbortedException, DbException,
	NoSuchElementException {
		while (it != null) {

			while (it.hasNext()) {
				Tuple t = it.next();
				if (t.getField(f.keyField()).compare(ipred.getOp(), ipred.getField())) {
					return t;
				}
				else if(ipred.getOp() == Op.LESS_THAN || ipred.getOp() == Op.LESS_THAN_OR_EQ) {
					// if the predicate was not satisfied and the operation is less than, we have
					// hit the end
					return null;
				}
				else if(ipred.getOp() == Op.EQUALS && 
						t.getField(f.keyField()).compare(Op.GREATER_THAN, ipred.getField())) {
					// if the tuple is now greater than the field passed in and the operation
					// is equals, we have reached the end
					return null;
				}
			}

			BPlusTreePageId nextp = curp.getRightSiblingId();
			// if there are no more pages to the right, end the iteration
			if(nextp == null) {
				return null;
			}
			else {
				curp = (BPlusTreeLeafPage) Database.getBufferPool().getPage(tid,
						nextp, Permissions.READ_ONLY);
				it = curp.iterator();
			}
		}

		return null;
	}

	/**
	 * rewind this iterator back to the beginning of the tuples
	 */
	public void rewind() throws DbException, TransactionAbortedException {
		close();
		open();
	}

	/**
	 * close the iterator
	 */
	public void close() {
		super.close();
		it = null;
	}
}
