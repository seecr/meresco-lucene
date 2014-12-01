package org.meresco.lucene.queries;


public class SegmentFieldKey {
	private Object segmentKey;
	private String keyName;
	
	
	public SegmentFieldKey(Object segmentKey, String keyName) {
		this.segmentKey = segmentKey;
		this.keyName = keyName;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((keyName == null) ? 0 : keyName.hashCode());
		result = prime * result
				+ ((segmentKey == null) ? 0 : segmentKey.hashCode());
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SegmentFieldKey other = (SegmentFieldKey) obj;
		if (keyName == null) {
			if (other.keyName != null)
				return false;
		} else if (!keyName.equals(other.keyName))
			return false;
		if (segmentKey == null) {
			if (other.segmentKey != null)
				return false;
		} else if (!segmentKey.equals(other.segmentKey))
			return false;
		return true;
	}
}
