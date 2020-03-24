package BPTree;

import java.util.Vector;

//TODO serializing overflowpages
public class OverflowPage {
	private OverflowPage next;
	private Vector<Ref> refs;
	private int maxSize; // node size
	public OverflowPage(int maxSize) {
		this.maxSize=maxSize;
//		refs = new RecordReference[maxSize];
		refs = new Vector<Ref>(maxSize);
		next = null;
	}
	public void addRecord(Ref recordRef) {
		if (refs.size()<maxSize) {
			refs.add(recordRef);
		}
		else {
			if (next==null) {
				next = new OverflowPage(maxSize);
			}			
			next.addRecord(recordRef);
		}
	}
	
	public boolean updateRef(int oldpage, int newpage) {
		int i=0;
		for (;i<refs.size()&&refs.get(i).getPage()<=oldpage;i++);
		//i--;
		if (i==0) {
			return false;
		}
		if (i<refs.size()) {
			refs.get(i-1).setPage(newpage);
		}
		if (i==refs.size()) {
			if (next!=null && next.updateRef(oldpage, newpage)) {
				return true;
			}
			else {
				refs.get(i-1).setPage(newpage);
			}
		}
		return false;
	}
	
}
