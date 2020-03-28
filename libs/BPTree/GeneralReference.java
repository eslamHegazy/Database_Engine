package BPTree;

import java.io.IOException;

import kalabalaDB.DBAppException;

public abstract class GeneralReference {
	public abstract boolean isOverflow();
	public abstract boolean isRecord();
	public abstract void updateRef(String oldpage, String newpage) throws DBAppException;
}
