package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
	private int[] histogram;
	private float[] bucketEnds;
	private final int buckets;
	private final int min;
	private final int max;
	private final float bucketRange;
	private final int range;
	private int numTuples;
	
	
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
    	histogram = new int[buckets];
    	bucketEnds = new float[buckets * 2];
    	this.buckets = buckets;
    	this.min = min;
    	this.max = max;
    	range = max - min;
    	bucketRange = (float) range / buckets;
    	makeBucketEnds();
    	numTuples = 0;
    }
    
    /**
     * 
     * @param value must be within min and max inclusive.
     * @return
     */
    public int bucketNum(int value){
    	if (value == max){
    		return buckets-1;
    	}
    	// 0-indexed
    	int bucketNum = (int) ((value - min) / bucketRange); 
    	return bucketNum;
    }
    
    public void makeBucketEnds(){
    	for (int i=0; i<buckets; i++){
    		bucketEnds[2*i] = min + bucketRange * i;
    		bucketEnds[2*i+1] = min + bucketRange * (i+1);
    	}
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	int bucketNum = bucketNum(v);
    	histogram[bucketNum]++;
    	numTuples++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	float selection = 0;
		int bucketNum = bucketNum(v);
		float equalsFreq = 0;
		if (v > min && v < max){
			float numIntInRange = (float) (Math.floor(bucketEnds[bucketNum*2+1]) - Math.floor(bucketEnds[bucketNum*2]));
			equalsFreq =  histogram[bucketNum] / numIntInRange; 
		}
    	
		// special equals case.
		if (op.equals(Predicate.Op.EQUALS)){
			if (v < min || v > max){
				return 0;
			}
    		selection += equalsFreq;
    	}
		
		else if (op.equals(Predicate.Op.NOT_EQUALS)){
			if (v < min || v > max){
				return 1;
			}
    		selection += equalsFreq;
    		return (1 - selection / numTuples);
		}
    	
		else if (op.equals(Predicate.Op.GREATER_THAN) || op.equals(Predicate.Op.GREATER_THAN_OR_EQ)){
			if (v > max){
				return 0;
			}
			if (v < min){
				return 1;
			}
			float high = bucketEnds[2*bucketNum + 1];
    		float partFreq = histogram[bucketNum] * (high - v) / bucketRange;
    		selection += partFreq;
    		for (int i=buckets-1; i > bucketNum; i--){
    			selection += histogram[i];
    		}
    		
    		if (op.equals(Predicate.Op.GREATER_THAN_OR_EQ)){
    			selection += equalsFreq;
    		}
			
		}
		
		// less than / less than or equal to
		else {
			if (v > max){
				return 1;
			}
			if (v < min){
				return 0;
			}
			float low = bucketEnds[2*bucketNum];
			float partFreq = histogram[bucketNum] * (v - low) / bucketRange;
    		selection += partFreq;
    		for (int i=0; i < bucketNum; i++){
    			selection += histogram[i];
    		}
    		if (op.equals(Predicate.Op.LESS_THAN_OR_EQ)){
    			selection += equalsFreq;
    		}    		
		}
		return selection / numTuples;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return histogram.toString();
    }
}
