package BPTree;

import java.util.Vector;


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
	
	public boolean updateRef(int pagenum) {
		int i=0;
		for (;i<refs.size()&&refs.get(i).getPage()<=pagenum;i++);
		//i--;
		if (i==0) {
			return false;
		}
		if (i<refs.size()) {
			refs.get(i-1).setPage(pagenum+1);
		}
		if (i==refs.size()) {
			if (next!=null && next.updateRef(pagenum)) {
				return true;
			}
			else {
				refs.get(i-1).setPage(pagenum+1);
			}
		}
		return false;
	}
	
}
