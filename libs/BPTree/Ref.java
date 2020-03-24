package BPTree;

import java.io.Serializable;

public class Ref extends GeneralReference implements Serializable{
	
	/**
	 * This class represents a pointer to the record. It is used at the leaves of the B+ tree 
	 */
	private static final long serialVersionUID = 1L;
	private int pageNo;//, indexInPage;
	
	public Ref(int pageNo, int indexInPage)
	{
		this.pageNo = pageNo;
//		this.indexInPage = indexInPage;
	}
	
	/**
	 * @return the page at which the record is saved on the hard disk
	 */
	public int getPage()
	{
		return pageNo;
	}
	
	public void setPage(int pageNo) {
		this.pageNo=pageNo;
	}
	
	/**
	 * @return the index at which the record is saved in the page
	 */
//	public int getIndexInPage()
//	{
//		return indexInPage;
//	}
//	
	public boolean isOverflow() {
		return false;
	}
	public boolean isRecord() {
		return true;
	}
	public String toString() {
		return pageNo+"";
	}

	
	
	public void updateRef(int oldpage, int newpage) {
		// TODO:
		
	}
}
