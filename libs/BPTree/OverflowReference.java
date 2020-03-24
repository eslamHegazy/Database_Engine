package BPTree;

public class OverflowReference extends GeneralReference {
	private OverflowPage firstPage;
	//TODO insert , delete and update pass the key and page
	
	public OverflowPage getFirstPage() {
		return firstPage;
	}
	public void setFirstPage(OverflowPage firstPage) {
		this.firstPage = firstPage;
	}
	public void insert(Ref recordRef) {
		firstPage.addRecord(recordRef);
	}
	public boolean isOverflow() {
		return true;
	}
	public boolean isRecord() {
		return false;
	}
	@Override
	public void updateRef(int oldpage, int newpage) {
		// TODO 
		firstPage.updateRef(oldpage, newpage);
		
	}
}

