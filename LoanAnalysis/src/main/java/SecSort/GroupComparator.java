package SecSort;

import org.apache.hadoop.io.WritableComparator;

public class GroupComparator extends WritableComparator {

	protected GroupComparator() {
		super(CompositeKeyWritable.class, true);
	}

	@Override
	public int compare(Object a, Object b) {
		
		CompositeKeyWritable ckw1 = (CompositeKeyWritable)a;
		CompositeKeyWritable ckw2 = (CompositeKeyWritable)b;
		
		return ckw1.getGrade().compareTo(ckw2.getGrade());
	}
	
}
