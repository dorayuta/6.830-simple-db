package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbFieldIndex;
    private final Type gbFieldType;
    private final int aFieldIndex;
    private final Op aggOperator;
    private final Map<Field, Integer> groupMap = new HashMap<Field, Integer>();
    // Map of group by number of tuples satisfying group
    private final Map<Field, Integer> countsByGroup = new HashMap<Field, Integer>();
    private int count;
    private int noGroupValue;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        gbFieldIndex = gbfield;
        gbFieldType = gbfieldtype;
        aFieldIndex = afield;
        aggOperator = what;
        count = 0;   
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	Field gbField = null;
    	IntField aField = (IntField) tup.getField(aFieldIndex);
    	int newValue = aField.getValue();
    	
    	// keep count of tuples.
    	count++;
    	
    	// if there is grouping
    	if (gbFieldIndex == NO_GROUPING){
    		if (count == 1){
    			noGroupValue = newValue;
    		}
    		else {
        		noGroupValue = getAggValue(newValue, noGroupValue);	
    		}
    	}
    	else {
    		gbField = tup.getField(gbFieldIndex);
    		if (groupMap.containsKey(gbField)){
    			int oldValue = groupMap.get(gbField);
    			int oldCount = countsByGroup.get(gbField);
    			oldCount++;
    			countsByGroup.put(gbField, oldCount);
    			groupMap.put(gbField, getAggValue(newValue, oldValue, oldCount));
    		}
    		else {
    			groupMap.put(gbField, newValue);
    			countsByGroup.put(gbField, 1);
    		}
    	}
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
    	List<Tuple> tupleList = new ArrayList<Tuple>();
    	
    	if (gbFieldIndex == NO_GROUPING){
    		Type[] typeArr = new Type[]{Type.INT_TYPE};
    		TupleDesc td = new TupleDesc(typeArr);
    		Tuple tuple = new Tuple(td);
    		int value = noGroupValue;
    		if (aggOperator == Aggregator.Op.AVG){
				value = value / count;
			}
    		tuple.setField(0, new IntField(value));
    		tupleList.add(tuple);
    		return new TupleIterator(td, tupleList);
    	}
    	
    	else {
    		Type[] typeArr = new Type[]{gbFieldType, Type.INT_TYPE};
    		TupleDesc td = new TupleDesc(typeArr);
    		for (Field gbField: groupMap.keySet()){
    			int value = groupMap.get(gbField);
    			if (aggOperator == Aggregator.Op.AVG){
    				int count = countsByGroup.get(gbField);
    				value = value / count;
    			}
    			IntField valueField = new IntField(value);
        		Tuple tuple = new Tuple(td);
    			tuple.setField(0, gbField);
    			tuple.setField(1, valueField);
    			tupleList.add(tuple);
    		}
    		return new TupleIterator(td, tupleList);
    	}
    }
    
   
    private int getAggValue(int newValue, int oldValue, int oldCount){
    	switch (this.aggOperator) {
    	case MIN:
    		return Math.min(newValue, oldValue);
    	case MAX:
    		return Math.max(newValue, oldValue);
    	case SUM:
    		return newValue + oldValue;
    	case AVG:
    		return newValue + oldValue;
    	case COUNT:
    		return oldCount;
    	case SUM_COUNT:
    		break;
    	case SC_AVG:
    		break;
    	}
    	return -1;
    }
    
    /**
     * Overload. Use total "count" as oldCount.
     * @param newValue
     * @param oldValue
     * @return
     */
    private int getAggValue(int newValue, int oldValue){
    	return getAggValue(newValue, oldValue, count);
    }

}
