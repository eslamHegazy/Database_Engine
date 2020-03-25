package BPTree;

import kalabalaDB.DBAppException;

public abstract class GeneralReference {
	public abstract boolean isOverflow();
	public abstract boolean isRecord();
	public abstract void updateRef(int oldpage, int newpage) throws DBAppException;
}
