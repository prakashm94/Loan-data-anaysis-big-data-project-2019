package SecSort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecodarySortComparator extends WritableComparator {

	protected SecodarySortComparator() {
		super(CompositeKeyWritable.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {

		CompositeKeyWritable ckw1 = (CompositeKeyWritable) a;
		CompositeKeyWritable ckw2 = (CompositeKeyWritable) b;

		int result = ckw1.getGrade().compareTo(ckw2.getGrade());

		if (result == 0) {
			return ckw1.getSubgrade().compareTo(ckw2.getSubgrade());
		}

		return result;
	}

	
	
}
