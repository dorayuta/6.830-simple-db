package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
        
        public Type getFieldType(){
        	return this.fieldType;
        }
        public String getFieldName(){
        	return this.fieldName;
        }
        
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return tdItemList.iterator();
    }

    private static final long serialVersionUID = 1L;
    private final List<TDItem> tdItemList = new ArrayList<TDItem>();
    private final int tdSize;
    
    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
    	tdSize = typeAr.length;
    	for (int i=0; i<tdSize; i++){
    		TDItem tdItem = new TDItem(typeAr[i], fieldAr[i]);
    		tdItemList.add(tdItem);
    	}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
    	tdSize = typeAr.length;
    	for (Type type: typeAr){
    		TDItem tdItem = new TDItem(type, null);
    		tdItemList.add(tdItem);
    	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return tdSize;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        return this.tdItemList.get(i).getFieldName();
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        return this.tdItemList.get(i).getFieldType();
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        
    	for (int i=0; i < this.tdSize; i++){
    		String fieldName = this.getFieldName(i);
        	if (fieldName != null && fieldName.equals(name)){
        		return i;
        	}
        }
        throw new NoSuchElementException("Field with name " + name + "not found." ); 
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
    	int size = 0;
    	for (TDItem tdItem: this.tdItemList){
    		size += tdItem.getFieldType().getLen();
    	}
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
    	int mergedNumFields = td1.numFields() + td2.numFields();
    	int td1NumFields = td1.numFields();
    	Type[] mergedTypeAr = new Type[mergedNumFields];
    	String[] mergedFieldAr = new String[mergedNumFields];
    	
    	for (int i=0; i<td1.numFields(); i++){
    		mergedTypeAr[i] = td1.getFieldType(i);
    		mergedFieldAr[i] = td1.getFieldName(i);
    	}
    	for (int j=0; j<td2.numFields(); j++){
    		mergedTypeAr[td1NumFields+j] = td2.getFieldType(j);
    		mergedFieldAr[td1NumFields+j] = td2.getFieldName(j);
    	}
    	return new TupleDesc(mergedTypeAr, mergedFieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if (!(o instanceof TupleDesc)){
        	return false;
        }
      	TupleDesc other = (TupleDesc) o;
      	if (this.getSize() != other.getSize()){
      		return false;
      	}
      	for (int i=0; i<this.numFields(); i++){
      		if (!this.getFieldType(i).equals(other.getFieldType(i))){
      			return false;
      		}
      	}
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
    	String fieldFormat = "%s[%d](%s[%d])";
    	StringBuilder sb = new StringBuilder();
    	for (int i=0; i<this.tdItemList.size(); i++){
    		TDItem tditem = tdItemList.get(i);
    		sb.append(String.format(fieldFormat, tditem.getFieldType().toString(), i, tditem.getFieldName(), i));
    	}
        return sb.toString();
    }
}
