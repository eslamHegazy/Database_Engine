package BPTree;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

import kalabalaDB.DBAppException;

public class OverflowReference extends GeneralReference implements Serializable 
	{
	private String firstPageName;
	//done (ta2riban) insert , delete and update pass the key and page
	
	public String getFirstPageName() {
		return firstPageName;
	}
	public OverflowPage getFirstPage() throws DBAppException {
		OverflowPage firstPage=deserializeOverflowPage(firstPageName);
		return firstPage;
	}
	public OverflowPage deserializeOverflowPage(String firstPageName2) throws DBAppException {
		try {
			FileInputStream fileIn = new FileInputStream("data/"+ this.firstPageName + ".class");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			OverflowPage OFP =   (OverflowPage) in.readObject();
			in.close();
			fileIn.close();
			return OFP;
		}
		catch(IOException e) {
			throw new DBAppException("IO Exception");
		}
		catch(ClassNotFoundException e) {
			throw new DBAppException("Class Not Found Exception");
		}
	}
	public void setFirstPage(OverflowPage firstPage) throws DBAppException, IOException {
		OverflowPage Page=firstPage;
		firstPageName=Page.getPageName();
		if(Page != null)
			Page.serialize();
	}
	public void insert(Ref recordRef) throws DBAppException, IOException {
		OverflowPage firstPage=deserializeOverflowPage(firstPageName);
		firstPage.addRecord(recordRef);
		firstPage.serialize();
	}
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		
		try
		{
			sb.append(deserializeOverflowPage(this.firstPageName));
		}
		catch(DBAppException e){
			System.out.println("WRONG first page name");
		}
		return sb.toString();
	}
	public boolean isOverflow() {
		return true;
	}
	public boolean isRecord() {
		return false;
	}
	@Override
	public void updateRef(int oldpage, int newpage) throws DBAppException {
		OverflowPage firstPage=deserializeOverflowPage(firstPageName);
		firstPage.updateRef(oldpage, newpage);
		firstPage.serialize();
		
	}
}

