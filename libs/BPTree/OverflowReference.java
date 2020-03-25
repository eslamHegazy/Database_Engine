package BPTree;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import kalabalaDB.DBAppException;

public class OverflowReference extends GeneralReference {
	private String firstPageName;
	//done (ta2riban) insert , delete and update pass the key and page
	
	public OverflowPage getFirstPage() throws DBAppException {
		OverflowPage firstPage=deserializeOverflowPage(firstPageName);
		return firstPage;
	}
	private OverflowPage deserializeOverflowPage(String firstPageName2) throws DBAppException {
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
	public void setFirstPage(OverflowPage firstPage) throws DBAppException {
		OverflowPage Page=firstPage;
		firstPageName=Page.getPageName();
		Page.serialize();
	}
	public void insert(Ref recordRef) throws DBAppException, IOException {
		OverflowPage firstPage=deserializeOverflowPage(firstPageName);
		firstPage.addRecord(recordRef);
		firstPage.serialize();
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

