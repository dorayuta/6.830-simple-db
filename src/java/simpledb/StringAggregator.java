package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbFieldIndex;
    private final Type gbFieldType;
    private final int aFieldIndex;
    private final Op aggOperator;
    private final Map<Field, Integer> groupMap = new HashMap<Field, Integer>();
    private int noGroupCount;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Aggregator.Op.COUNT){
        	throw new IllegalArgumentException("Aggregate operater must be count.");
        }
    	gbFieldIndex = gbfield;
        gbFieldType = gbfieldtype;
        aFieldIndex = afield;
        aggOperator = what;
        noGroupCount = 0;   
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	Field gbField = null;    	
    	// keep count of tuples.
    	noGroupCount++;
    	
    	// if there is grouping
    	if (gbFieldIndex == NO_GROUPING){
    		noGroupCount++;
    	}
    	else {
    		gbField = tup.getField(gbFieldIndex);
    		if (groupMap.containsKey(gbField)){
    			int oldValue = groupMap.get(gbField);
    			oldValue++;
    			groupMap.put(gbField, oldValue);
    		}
    		else {
    			groupMap.put(gbField, 1);
    		}
    	}
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
    	List<Tuple> tupleList = new ArrayList<Tuple>();
    	
    	if (gbFieldIndex == NO_GROUPING){
    		Type[] typeArr = new Type[]{Type.INT_TYPE};
    		TupleDesc td = new TupleDesc(typeArr);
    		Tuple tuple = new Tuple(td);
    		tuple.setField(0, new IntField(noGroupCount));
    		tupleList.add(tuple);
    		return new TupleIterator(td, tupleList);
    	}
    	
    	else {
    		Type[] typeArr = new Type[]{gbFieldType, Type.INT_TYPE};
    		TupleDesc td = new TupleDesc(typeArr);
    		for (Field gbField: groupMap.keySet()){
    			IntField value = new IntField(groupMap.get(gbField));
        		Tuple tuple = new Tuple(td);
    			tuple.setField(0, gbField);
    			tuple.setField(1, value);
    			tupleList.add(tuple);
    		}
    		return new TupleIterator(td, tupleList);
    	}
    }

}
