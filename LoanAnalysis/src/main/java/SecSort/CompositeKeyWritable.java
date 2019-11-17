package SecSort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKeyWritable implements WritableComparable<CompositeKeyWritable> {

	private String grade;
	private String subgrade;

	public CompositeKeyWritable() {
		super();
	}

	public CompositeKeyWritable(String grade, String id) {
		super();
		this.grade = grade;
		this.subgrade = id;
	}

	public void write(DataOutput out) throws IOException {

		out.writeUTF(grade);
		out.writeUTF(subgrade);
	}

	public void readFields(DataInput in) throws IOException {

		grade = in.readUTF();
		subgrade = in.readUTF();
	}

	public int compareTo(CompositeKeyWritable o) {
		int result = this.grade.compareTo(o.grade);
		if (result == 0) {
			return this.subgrade.compareTo(o.subgrade);
		}
		return result;
	}

	public String getGrade() {
		return grade;
	}

	public void setGrade(String grade) {
		this.grade = grade;
	}

	public String getSubgrade() {
		return subgrade;
	}

	public void setSubgrade(String subgrade) {
		this.subgrade = subgrade;
	}

	@Override
	public String toString() {

		return grade + "," + subgrade;
	}

}
