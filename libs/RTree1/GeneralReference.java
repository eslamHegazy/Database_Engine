package RTree1;

import java.io.IOException;
import java.util.ArrayList;

import kalabalaDB.DBAppException;

public abstract class GeneralReference {
	public abstract boolean isOverflow();
	public abstract boolean isRecord();
	public abstract void updateRef(String oldpage, String newpage, int tableNameLength) throws DBAppException;
	
	public ArrayList<Ref> getALLRef() throws DBAppException
	{
		ArrayList<Ref> results = new ArrayList<Ref>();
		if(this instanceof Ref)
		{
			results.add((Ref)this);
		}
		else
		{
			OverflowReference ov = (OverflowReference) this;
			results.addAll(ov.ALLgetRef());
		}
		
		return results;
		
	}
}
