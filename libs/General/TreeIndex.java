package General;

import java.io.IOException;
import java.util.ArrayList;

import BPTree.BPTreeLeafNode;
import kalabalaDB.DBAppException;
import kalabalaDB.Polygons;

public interface TreeIndex<T extends Comparable<T>> {
	public GeneralReference search(T key) throws DBAppException;
	public Ref searchForInsertion(T key,int tableLength) throws DBAppException ;
	public boolean delete(T key) throws DBAppException;
	public boolean delete(T key, String Page_name) throws DBAppException;
	public void insert(T key, Ref recordReference) throws DBAppException;
	public LeafNode getLeftmostLeaf() throws DBAppException ;
	public ArrayList<GeneralReference> searchMTE(T key) throws DBAppException;
	public ArrayList<GeneralReference> searchMT(T key) throws DBAppException;
//	public ArrayList<GeneralReference> searchlTE(T key) throws DBAppException;
//	public ArrayList<GeneralReference> searchlT(T key) throws DBAppException;
	public void updateRef(String oldpage,String newpage,T key, int tableNameLength) throws DBAppException;
}
