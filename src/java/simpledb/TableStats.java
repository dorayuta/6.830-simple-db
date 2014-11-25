package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    
    // 0 index of mapped int[] is min, 1 is max.
    private Map<Integer, IntHistogram> intFieldToHistogram;
    private Map<Integer, StringHistogram> stringFieldToHistogram;
    private int numTuples;
    private final int ioCostPerPage;
    private final int tableid;
    private int numPages;
    
    
    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
    	this.tableid = tableid;
    	this.ioCostPerPage = ioCostPerPage;
    	
    	Map<String, int[]> fieldsMinMaxMap = new HashMap<String, int[]>();
    	numTuples = 0;
    	numPages = 0;

    	intFieldToHistogram = new HashMap<Integer, IntHistogram>();
    	stringFieldToHistogram = new HashMap<Integer, StringHistogram>();
    	
    	DbFile table = Database.getCatalog().getDatabaseFile(tableid);
    	TupleDesc td = table.getTupleDesc();
    	int numFields = td.numFields();
    		
    	TransactionId scanTid = new TransactionId();
    	DbFileIterator iter = table.iterator(scanTid);
		try {
			iter.open();
			// initialize min and max
			if (iter.hasNext()){
				Tuple tup = iter.next();
				numTuples++;
				// pre-populate map
				for (int i=0; i<numFields; i++){
					if (td.getFieldType(i).equals(Type.INT_TYPE)){
						String fieldName = td.getFieldName(i);
						IntField tupleField = (IntField) tup.getField(i);
						int[] minMax = new int[]{tupleField.getValue(), tupleField.getValue()};
						fieldsMinMaxMap.put(fieldName, minMax);
					}
				}
			}
			while (iter.hasNext()){
				Tuple tup = iter.next();
				numTuples++;
				// update mins and maxes.
				for (int i=0; i<numFields; i++){
					if (td.getFieldType(i).equals(Type.INT_TYPE)){
						String fieldName = td.getFieldName(i);
						IntField tupleField = (IntField) tup.getField(i);
						int value = tupleField.getValue();
						int[] minMax = fieldsMinMaxMap.get(fieldName);
						if (minMax[0] > value){
							minMax[0] = value;
						}
						else if (minMax[1] < value){
							minMax[1] = value;
						}
						// not necessary update?
						fieldsMinMaxMap.put(fieldName, minMax);
					}
				}
			}
			iter.close();
			
		} catch (DbException | TransactionAbortedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// initialize histograms
		for (int i=0; i<numFields; i++){
			if (td.getFieldType(i).equals(Type.INT_TYPE)){
				int[] minMax = fieldsMinMaxMap.get(td.getFieldName(i));
				IntHistogram intHistogram = new IntHistogram(NUM_HIST_BINS, minMax[0], minMax[1]);
				intFieldToHistogram.put(i, intHistogram);
			}
			else if (td.getFieldType(i).equals(Type.STRING_TYPE)){
				StringHistogram stringHistogram = new StringHistogram(NUM_HIST_BINS);
				stringFieldToHistogram.put(i, stringHistogram);
			}
		}
		
		// populate histograms
    	DbFileIterator histogramIter = table.iterator(scanTid);
    	try {
        	histogramIter.open();
    		Tuple tup = null;
			while (histogramIter.hasNext()){
				tup = histogramIter.next();
				for (int i=0; i<numFields; i++){
					if (intFieldToHistogram.containsKey(i)){
						IntField field = (IntField) tup.getField(i);
						intFieldToHistogram.get(i).addValue(field.getValue());
					}
					else {
						StringField field = (StringField) tup.getField(i);
						stringFieldToHistogram.get(i).addValue(field.getValue());
					}
				}				
			}
			if (tup != null){
				numPages = tup.getRecordId().getPageId().pageNumber();
			}
			histogramIter.close();
		} catch (NoSuchElementException | DbException
				| TransactionAbortedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() { 
        return numPages * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int) (numTuples * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
    	if (intFieldToHistogram.containsKey(field)){
    		IntHistogram histogram = intFieldToHistogram.get(field);
    		IntField intField = (IntField) constant;
    		return (double) histogram.estimateSelectivity(op, intField.getValue());
    	}
    	else {
    		StringHistogram histogram = stringFieldToHistogram.get(field);
    		StringField stringField = (StringField) constant;
    		return (double) histogram.estimateSelectivity(op, stringField.getValue());
    	}
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return 0;
    }

}
