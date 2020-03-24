package BPTree;

public class OverflowReference extends GeneralReference {
	private OverflowPage firstPage;
	//TODO insert , delete and update pass the key and page
	
	public void insert(Ref recordRef) {
		firstPage.addRecord(recordRef);
	}
	public boolean isOverflow() {
		return true;
	}
	public boolean isRecord() {
		return false;
	}
}

