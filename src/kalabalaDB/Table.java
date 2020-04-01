package kalabalaDB;

import java.io.*;
import java.util.*;

import BPTree.BPTree;
import BPTree.BPTreeLeafNode;
import BPTree.GeneralReference;
import BPTree.OverflowPage;
import BPTree.OverflowReference;
import BPTree.Ref;

public class Table implements Serializable {
	/**
	 * 
	 */

	private Vector<String> pages = new Vector();
	private int MaximumRowsCountinPage;
	private Vector<Object> min = new Vector<>();
	private Vector<Object> max = new Vector<>();
	private String tableName;
	private String strClusteringKey;
	private int primaryPos;

	private Hashtable<String, BPTree> colNameBTreeIndex = new Hashtable<>();

	public Hashtable<String, BPTree> getColNameBTreeIndex() {
		return colNameBTreeIndex;
	}
	public Ref searchWithCluster(Comparable key, BPTree b) throws DBAppException {
		Ref ref = b.searchRequiredReference(key); // NOT IMPLEMENTED
		if (ref == null) { // returns null if key is the least value in tree
			return new Ref(pages.get(0));
		}
		return ref;
	}

	public void printIndices() {
		for (String x : colNameBTreeIndex.keySet()) {
			BPTree b = colNameBTreeIndex.get(x);
			System.out.println(x);
			System.out.println(b);
			System.out.println();
		}
	}

	public Vector<String> getPages() {
		return pages;
	}

	public String getNewPageName() {
		return tableName + ((pages.size() == 0) ? 0
				: Integer.parseInt((pages.get(pages.size() - 1)).substring(tableName.length())) + 1);

	}

	public int getMaximumRowsCountinPage() {
		return MaximumRowsCountinPage;
	}

	public void setMaximumRowsCountinPage(int maximumRowsCountinPage) {
		MaximumRowsCountinPage = maximumRowsCountinPage;
	}

	public void setPages(Vector<String> pages) {
		this.pages = pages;
	}

	public String getStrClusteringKey() {
		return strClusteringKey;
	}

	public Vector<Object> getMin() {
		return min;
	}

	public void setMin(Vector<Object> min) {
		this.min = min;
	}

	public Vector<Object> getMax() {
		return max;
	}

	public void setMax(Vector<Object> max) {
		this.max = max;
	}

	public void setStrClusteringKey(String strClusteringKey) {
		this.strClusteringKey = strClusteringKey;
	}

	// private transient Vector colNames;
	// private transient Vector colTypes;

	public Table() {

	}

	public void setTableName(String name) {
		tableName = name;
	}

	public void setPrimaryPos(int pos) {
		primaryPos = pos;
	}

	public int getPrimaryPos() {
		return primaryPos;
	}

	public String getTableName() {
		return tableName;
	}

	public static Page deserialize(String name) throws DBAppException {
		try {
			FileInputStream fileIn = new FileInputStream("data/" + name + ".class");
			// FileInputStream fileIn = new FileInputStream("data/"+name + ".ser");
			// TODO: Check resulting path + check class/ ser
			ObjectInputStream in = new ObjectInputStream(fileIn);
			Page xx = (Page) in.readObject();
			in.close();
			fileIn.close();
			return xx;
		} catch (IOException e) {
			throw new DBAppException("IO Exception");
		} catch (ClassNotFoundException e) {
			throw new DBAppException("Class Not Found Exception");
		}
	}

	public static void addInVector(Vector<Object> vs, Object str, int n) {
		vs.insertElementAt(str, n);
	}


	public void addInPage(int curr, Tuple x, String keyType, String keyColName, int nodeSize) throws DBAppException, IOException {
		// System.out.println(x+" "+curr);\

		if (curr < pages.size()) {
			String pageName = pages.get(curr);

			Page p = deserialize(pageName);
			if (p.size() < MaximumRowsCountinPage) {
				// System.out.println("blboz2");
				p.insertIntoPage(x, primaryPos);
				// System.out.println("blboz3");

				Object minn = p.getTuples().get(0).getAttributes().get(primaryPos);
				Object maxx = p.getTuples().get(p.size() - 1).getAttributes().get(primaryPos);
				min.remove(curr);
				addInVector(min, minn, curr);
				max.remove(curr);
				addInVector(max, maxx, curr);
				
				if (colNameBTreeIndex.containsKey(keyColName)) {
					BPTree bTree = colNameBTreeIndex.get(keyColName);
					Ref recordReference = new Ref(p.getPageName());
					bTree.insert((Comparable) x.getAttributes().get(primaryPos), recordReference);
					colNameBTreeIndex.put(keyColName, bTree);

				}
				p.serialize();
			} else {
				// Tuple t=p.getTuples().get(p.size()-1);//element 199
				p.insertIntoPage(x, primaryPos);
				if (colNameBTreeIndex.containsKey(keyColName)) {
					BPTree bTree = colNameBTreeIndex.get(keyColName);
					Ref recordReference = new Ref(p.getPageName());
					bTree.insert((Comparable) x.getAttributes().get(primaryPos), recordReference);
					colNameBTreeIndex.put(keyColName, bTree);
				}
				Tuple t = p.getTuples().remove(p.size() - 1);

				if (colNameBTreeIndex.containsKey(keyColName)) {
					BPTree bTree = colNameBTreeIndex.get(keyColName);

					bTree.delete((Comparable) t.getAttributes().get(primaryPos));
				}
				Object minn = p.getTuples().get(0).getAttributes().get(primaryPos);
				Object maxx = p.getTuples().get(p.size() - 1).getAttributes().get(primaryPos);
				min.remove(curr);
				addInVector(min, minn, curr);
				max.remove(curr);
				addInVector(max, maxx, curr);

				p.serialize();
				addInPage(curr + 1, t, keyType, keyColName, nodeSize);
			}
		} else {
			Page p = new Page(getNewPageName());
			p.insertIntoPage(x, primaryPos);
			if (colNameBTreeIndex.containsKey(keyColName)) {
				BPTree bTree = colNameBTreeIndex.get(keyColName);
				Ref recordReference = new Ref(p.getPageName());
				bTree.insert((Comparable) x.getAttributes().get(primaryPos), recordReference);
				colNameBTreeIndex.put(keyColName, bTree);
			}
			Object keyValue = p.getTuples().get(0).getAttributes().get(primaryPos);
			pages.addElement(p.getPageName());
			min.addElement(keyValue);
			max.addElement(keyValue);
			p.serialize();

		}

	}

//	public static void createBPTreeGivinType(BPTree bTree,String colType,int nodeSize) throws DBAppException{
//		switch(colType){
//		case "java.lang.Integer":bTree=new BPTree<Integer>(nodeSize);break;
//		case "java.lang.Double":bTree=new BPTree<Double>(nodeSize);break;
//		case "java.util.Date":bTree=new BPTree<Date>(nodeSize);break;
//		case "java.lang.Boolean":bTree=new BPTree<Boolean>(nodeSize);break;
//		case "java.awt.Polygon
//	":bTree=new BPTree<Polygons>(nodeSize);break;
//		default :throw new DBAppException("I've never seen this colType in my life");
//		}
//	}

	public void insertSorted(Tuple x, Object keyV,String keyType,String keyColName,int nodeSize,ArrayList colNames) throws DBAppException, IOException{
		
		if(pages.size()==0){
			Page p=new Page(getNewPageName());
			p.insertIntoPage(x, primaryPos);
			if (colNameBTreeIndex.containsKey(keyColName)) {
				BPTree bTree = colNameBTreeIndex.get(keyColName);
				Ref recordReference = new Ref(p.getPageName());
				bTree.insert((Comparable) x.getAttributes().get(primaryPos), recordReference);
				colNameBTreeIndex.put(keyColName, bTree);
			}
			pages.addElement(p.getPageName());
			min.addElement(keyV);
			max.addElement(keyV);
			p.serialize();

		} else {
			Comparable keyValue = (Comparable) keyV;
			if (colNameBTreeIndex.containsKey(keyColName)) {
				BPTree tree = colNameBTreeIndex.get(keyColName);
				Ref pageReference = tree.searchForInsertion(keyValue);
				String pageName = this.tableName + pageReference.getPage();
				int curr = pages.indexOf(pageName);
				addInPage(curr, x, keyType, keyColName, nodeSize);
			} else {
				int lower = 0;
				int upper = min.size();
				int curr = 0;
				for (curr = 0; curr < pages.size(); curr++) {
					Object minn = (min.get(curr));
					Object maxx = max.get(curr);
					if ((keyValue.compareTo(minn) >= 0 && keyValue.compareTo(maxx) <= 0)
							|| (keyValue.compareTo(minn) < 0) || curr == pages.size() - 1) {
						addInPage(curr, x, keyType, keyColName, nodeSize);
						break;
					}
				}
			}
		}

		Set<String> c = colNameBTreeIndex.keySet();
		for (int i = 0; i < c.size(); i++) {
			if (!keyColName.equals(c)) {
				BPTree tree = colNameBTreeIndex.get(c);
				int index = 0;
				for (; index < colNames.size(); index++) {
					if (keyColName.equals(colNames.get(index))) {
						break;
					}
				}
				Object keyValueOfNonCluster = x.getAttributes().get(index);
				Ref pageReference = tree.searchForInsertion((Comparable) keyValueOfNonCluster);
				tree.insert((Comparable) keyValueOfNonCluster, pageReference);
			}
		}
	}

	@SuppressWarnings("unchecked")
	public void deleteInTable(Hashtable<String, Object> htblColNameValue, Vector<String[]> metaOfTable,
			String clusteringKey) throws DBAppException {

		if (invalidDelete(htblColNameValue, metaOfTable)) {
			throw new DBAppException("false operation");
			// TODO: Is this message appropriate?
		}

		ArrayList<String> indicesGiven = indicesIHave(htblColNameValue, colNameBTreeIndex);
		ArrayList<String> allIndices = allTableIndices(colNameBTreeIndex);

		if (!(indicesGiven.size() == 0)) {
			String selectedCol = (clusteringKey != null && clusteringKeyHasIndex(indicesGiven, clusteringKey))
					? clusteringKey
					: indicesGiven.get(0);
			boolean isCluster = selectedCol.equals(clusteringKey);

			BPTree bpTree = colNameBTreeIndex.get(selectedCol);
			GeneralReference pageReference = bpTree.search((Comparable) htblColNameValue.get(selectedCol));
			if (pageReference instanceof Ref) {
				Ref x = (Ref) pageReference;
				Page p = deserialize(x.getPage() + "");
				p.deleteInPageforRef(metaOfTable, primaryPos, selectedCol, colNameBTreeIndex, htblColNameValue,
						allIndices, isCluster);
				setMinMax(p);

			} else {
				OverflowReference x = (OverflowReference) pageReference;
				OverflowPage OFP = x.deserializeOverflowPage(x.getFirstPageName());
				Set<Ref> allReferences = deleteFromBPTree(OFP);
				OFP.serialize();
				for (Ref ref : allReferences) {
					Page p = deserialize(ref.getPage() + "");
					p.deleteInPageforRef(metaOfTable, primaryPos, selectedCol, colNameBTreeIndex, htblColNameValue,
							allIndices, isCluster);
					setMinMax(p);
				}

			}

		} else if (clusteringKey != null) {
			for (int i = 0; i < pages.size(); i++) {
				if (((Comparable) htblColNameValue.get(clusteringKey)).compareTo(((Comparable) min.get(i))) < 0) {
					break;
				}
				if (((Comparable) htblColNameValue.get(clusteringKey)).compareTo(((Comparable) max.get(i))) >= 0
						&& ((Comparable) htblColNameValue.get(clusteringKey))
								.compareTo(((Comparable) max.get(i))) <= 0) {

					Page page = deserialize(pages.get(i));
					page.deleteInPageWithBS(htblColNameValue, metaOfTable, clusteringKey, primaryPos);
					if (page.getTuples().size() == 0) {
						File f = new File("data/" + page.getPageName() + ".class");
						f.delete();
						pages.remove(i);
						min.remove(i);
						max.remove(i);
						i--;

					} else {
						Object minn = page.getTuples().get(0).getAttributes().get(primaryPos);
						Object maxx = page.getTuples().get(page.size() - 1).getAttributes().get(primaryPos);
						min.setElementAt(minn, i);
						max.setElementAt(maxx, i);
						page.serialize();
					}
					{
					}

				}
			}
		} else {
			Vector<Integer> attributeIndex = new Vector();
			Set<String> keys = htblColNameValue.keySet();
			for (String key : keys) {
				int i;
				for (i = 0; i < metaOfTable.size(); i++) {
					if (metaOfTable.get(i)[1].equals(key)) {
						break;
					}
				}
				System.out.println(i);
				attributeIndex.add(i);
			}
			for (int i = 0; i < pages.size(); i++) {
				String pageName = pages.get(i);
				Page p = deserialize(pageName);
				try {
					FileInputStream fileIn = new FileInputStream("data/" + pageName + ".class");
					ObjectInputStream in = new ObjectInputStream(fileIn);
					Page xx = (Page) in.readObject();
					in.close();
					fileIn.close();
				} catch (ClassNotFoundException e) {
					throw new DBAppException("Class Not Found Exception");
				} catch (IOException e) {
					throw new DBAppException("IO Exception");
				}
				p.deleteInPage(htblColNameValue, attributeIndex);
				if (p.getTuples().size() == 0) {
					File f = new File("data/" + pageName + ".class");
					f.delete();
					pages.remove(i);
					min.remove(i);
					max.remove(i);
					i--;

				} else {
					Object minn = p.getTuples().get(0).getAttributes().get(primaryPos);
					Object maxx = p.getTuples().get(p.size() - 1).getAttributes().get(primaryPos);
					min.setElementAt(minn, i);
					max.setElementAt(maxx, i);
					p.serialize();
				}
			}
		}

	}

	public Set<Ref> deleteFromBPTree(OverflowPage OFP) throws DBAppException {
		Set<Ref> allReferences = new HashSet<>();
		allReferences.addAll(OFP.getRefs());
		OverflowPage nextOFP;
		boolean notNull = true;
		if (OFP.getNext() != null) {
			nextOFP = OFP.deserialize(OFP.getNext());
			while (notNull) {
				allReferences.addAll(nextOFP.getRefs());
				if (nextOFP.getNext() != null) {
					nextOFP = nextOFP.deserialize(nextOFP.getNext());
				} else {
					notNull = false;
				}
				nextOFP.serialize();
			}
		}
		return allReferences;

	}

	public boolean invalidDelete(Hashtable<String, Object> htblColNameValue, Vector<String[]> metaOfTable)
			throws DBAppException {
		Set<String> keys = htblColNameValue.keySet();
		for (String key : keys) {
			int i;
			for (i = 0; i < metaOfTable.size(); i++) {
				if (metaOfTable.get(i)[1].equals(key)) {
					try {
						Class colType = Class.forName(metaOfTable.get(i)[2]);
						Class parameterType = htblColNameValue.get(key).getClass();
						Class polyOriginal = Class.forName("java.awt.Polygon");
						if (colType == polyOriginal) {
							colType = Class.forName("kalabalaDB.Polygons");
						}
						System.out.println(colType + " " + parameterType);
						if (!colType.equals(parameterType))
							return true;
						else
							break;
					} catch (ClassNotFoundException e) {
						throw new DBAppException("Class Not Found Exception");
					}
				}
			}
			if (i  == metaOfTable.size())
				return true;
		}
		return false;
	}

	public void setMinMax(Page p) throws DBAppException {
		String pageName = p.getPageName();
		for (int i = 0; i < pages.size(); i++) {
			if (pages.get(i).equals(pageName)) {
				if (p.getTuples().size() == 0) {
					File f = new File("data/" + pageName + ".class");
					f.delete();
					pages.remove(i);
					min.remove(i);
					max.remove(i);
					i--;

				} else {
					Object minn = p.getTuples().get(0).getAttributes().get(primaryPos);
					Object maxx = p.getTuples().get(p.size() - 1).getAttributes().get(primaryPos);
					min.setElementAt(minn, i);
					max.setElementAt(maxx, i);
					p.serialize();
				}
			}
		}
	}

	public ArrayList<String> indicesIHave(Hashtable<String, Object> htblColNameValue,
			Hashtable<String, BPTree> colNameBTreeIndex) {
		ArrayList<String> columns = new ArrayList<String>();

		Set<String> keys = htblColNameValue.keySet();
		for (String key : keys) {
			columns.add(key);
		}

		ArrayList<String> indices = new ArrayList<String>();

		Set<String> keys1 = colNameBTreeIndex.keySet();
		for (String key1 : keys1) {
			indices.add(key1);
		}
		ArrayList<String> indicesGiven = new ArrayList<String>();
		for (int i = 0; i < indices.size(); i++) {
			for (int j = 0; j < columns.size(); j++) {
				if (indices.get(i) == columns.get(j)) {
					indicesGiven.add(columns.get(j));

				}
			}
		}
		return indicesGiven;
	}

	public ArrayList<String> allTableIndices(Hashtable<String, BPTree> colNameBTreeIndex) {
		ArrayList<String> allTableIndices = new ArrayList<String>();
		Set<String> keys = colNameBTreeIndex.keySet();
		for (String key : keys) {
			allTableIndices.add(key);
		}

		return allTableIndices;
	}


	public boolean clusteringKeyHasIndex(ArrayList<String> indices, String clusteringKey) {
		if (clusteringKey != null) {
			for (int i = 0; i < indices.size(); i++) {
				if (indices.get(i) == clusteringKey) {
					return true;
				}
			}
			return false;
		} else
			return false;

	}

	public void createBTreeIndex(String strColName, BPTree bTree, int colPosition) throws DBAppException, IOException {
		if (colNameBTreeIndex.containsKey(strColName)) {
			throw new DBAppException("BTree index already exists on this column");
		} else {
			colNameBTreeIndex.put(strColName, bTree);
		}
		for (String str : pages) {
			Page p = deserialize(str);
			
			int i = 0;
			for (Tuple t : p.getTuples()) {
				Ref recordReference = new Ref(p.getPageName());
				bTree.insert((Comparable) t.getAttributes().get(colPosition), recordReference);
				i++;
			}
			p.serialize();
		}
	}

	public static int getIndexNumber(String pName, int s) {
		String num = pName.substring(s);
		return Integer.parseInt(num);
	}

	public Iterator<Tuple> selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators,Vector<String[]> metaOfTable) throws DBAppException {
		
		//check columns names and types validity
		checkQueryValidity(arrSQLTerms,metaOfTable);
		
		//this is for complete linear/binary search through the whole table
		//Iterator<Tuple> s=inspectCols(arrSQLTerms,strarrOperators,metaOfTable);//method inspect cols determines which way to search through pages
		//strarroperator operators there are checked in method setOperation and getArrayOfTuples
		ArrayList<Tuple>current=new ArrayList(),next=new ArrayList();
		int i=0;
		for(SQLTerm x:arrSQLTerms) {
			if(i==0) {
				current=getArrayOfTuples(x._strColumnName,x._objValue,x._strOperator);
				i++;
				continue;
			}
			
				next=getArrayOfTuples(x._strColumnName,x._objValue,x._strOperator);
				current=setOperation(current,next,strarrOperators[i++-1]);
			
		}
		return current.iterator();
	}
	
	private ArrayList<Tuple> setOperation(ArrayList<Tuple> current, ArrayList<Tuple> next, String string) throws DBAppException {
		string=string.toLowerCase();
		ArrayList<Tuple> res=new ArrayList();
		switch(string) {
		case "or": res=orSets(current,next);break;
		case "and":res=andSets(current,next);break;
		case "xor":res=xorSets(current,next);break;
		default:throw new DBAppException("wrong operation type "+string);
		}
		return res;
	}
	private ArrayList<Tuple> xorSets(ArrayList<Tuple> current, ArrayList<Tuple> next) {
		return differenceSets(orSets(current,next),andSets(current,next));
	}
	private ArrayList<Tuple> differenceSets(ArrayList<Tuple> A, ArrayList<Tuple> B) {
		ArrayList<Tuple> res = new ArrayList<>();
		
		HashSet<Tuple> first= new HashSet<>();
		HashSet<Tuple> second= new HashSet<>();
		
		for (int i=0;i<B.size();i++) {
			Tuple cur = B.get(i);
			second.add(cur);
		}
		for (int i=0;i<A.size();i++) {
			Tuple cur = A.get(i);
			if (first.contains(cur)) continue;
			first.add(cur);
			if (!second.contains(cur)) {
				res.add(cur);
			}
		}
		return res;
	}
	private ArrayList<Tuple> andSets(ArrayList<Tuple> current, ArrayList<Tuple> next) {
		ArrayList<Tuple> res = new ArrayList<>();
		
		HashSet<Tuple> first= new HashSet<>();
		HashSet<Tuple> second= new HashSet<>();
		for (int i=0;i<current.size();i++) {
			Tuple cur = current.get(i);
			first.add(cur);
		}
		for (int i=0;i<next.size();i++) {
			Tuple cur = next.get(i);
			if (!second.contains(cur)) {
				second.add(cur);
				if (first.contains(cur)) {
					res.add(cur);
				}
			}
		}
		return res;
	}
	private ArrayList<Tuple> orSets(ArrayList<Tuple> current, ArrayList<Tuple> next) {
		Set<Tuple> x=new HashSet();
		for(Tuple t:current)
			x.add(t);
		for(Tuple t:next)
			x.add(t);
		ArrayList<Tuple> res=new ArrayList();
		for(Tuple t:x)
			res.add(t);
		return res;
	}
	private ArrayList<Tuple> getArrayOfTuples(String _strColumnName, Object _objValue, String _strOperator) throws DBAppException {
		if(!validOp(_strOperator)) {
			throw new DBAppException("Wrong operator type "+_strOperator);
		}
		int pos=getColPositionWithinTuple(_strColumnName);
		if(_strOperator.equals("!="))
			return goLinear(_strColumnName,_objValue,_strOperator,pos);
            
		
		return (colNameBTreeIndex.containsKey(_strColumnName))?goWithIndex(_strColumnName,_objValue,_strOperator,pos):
			(_strColumnName.equals(strClusteringKey))?goBinary(_strColumnName,_objValue,_strOperator,pos):goLinear(_strColumnName,_objValue,_strOperator,pos);
	}
	private int getColPositionWithinTuple(String _strColumnName) {
		// TODO Auto-generated method stub
		return 0;
	}
	private boolean validOp(String _strOperator) {
		return _strOperator.equals("=")||_strOperator.equals("!=")||_strOperator.equals(">")||_strOperator.equals(">=")||_strOperator.equals("<")||_strOperator.equals("<=");
		
	}
	private ArrayList<Tuple> goLinear(String _strColumnName, Object _objValue, String _strOperator, int pos) throws DBAppException {
	//TODO
		ArrayList<Tuple> res=new ArrayList();
		switch(_strOperator) {
		case ">":
		case ">=": res=mtOrMtlLinear( _strColumnName,  _objValue, _strOperator,pos);break;
		case "<":
		case "<=":res=ltOrLtlLinear(_strColumnName,  _objValue, _strOperator,pos);break;
		case "=":res=equalsLinear(_strColumnName,  _objValue, _strOperator,pos);break;
		case "!=": res=notEqualsLinear(_strColumnName,  _objValue, _strOperator,pos);break;
		}
		return res;
	}
	private ArrayList<Tuple> notEqualsLinear(String _strColumnName, Object _objValue, String _strOperator, int pos) {
		// TODO Auto-generated method stub
		return null;
	}
	private ArrayList<Tuple> equalsLinear(String _strColumnName, Object _objValue, String _strOperator, int pos) {
		// TODO Auto-generated method stub
		return null;
	}
	private ArrayList<Tuple> ltOrLtlLinear(String _strColumnName, Object _objValue, String _strOperator,int pos) throws DBAppException {
        
		ArrayList<Tuple> res=new ArrayList();
		for(int i=0;i<pages.size();i++) {
			if(((Comparable) min.get(i)).compareTo((Comparable)_objValue)>0)break;
			if(((Comparable) min.get(i)).compareTo((Comparable)_objValue)==0&&_strOperator.length()==1)break;
			Page x=deserialize(pages.get(i));
			int j=0;
			while(j<x.getTuples().size()&&((Comparable)x.getTuples().get(j).getAttributes().get(pos)).compareTo((Comparable)_objValue)==00) 
			       	res.add(x.getTuples().get(j++));
			if(_strOperator.length()==2) {
				while(j<x.getTuples().size()&&((Comparable)x.getTuples().get(j).getAttributes().get(pos)).compareTo((Comparable)_objValue)<0) 
			       	res.add(x.getTuples().get(j++));
			}

		}
		return res;
		
	}

	private ArrayList<Tuple> mtOrMtlLinear(String _strColumnName, Object _objValue, String _strOperator, int pos) throws DBAppException {
		ArrayList<Tuple> res=new ArrayList();
		for(int i=pages.size()-1;i>=0;i++) {
			if(((Comparable) max.get(i)).compareTo((Comparable)_objValue)<0)break;
			if(((Comparable) max.get(i)).compareTo((Comparable)_objValue)==0&&_strOperator.length()==1)break;
			Page x=deserialize(pages.get(i));
			int j=x.getTuples().size()-1;
			while(j>=0&&((Comparable)x.getTuples().get(j).getAttributes().get(pos)).compareTo((Comparable)_objValue)>0) {
				res.add(0, x.getTuples().get(j));
			    j--;
			}
			if(_strOperator.length()==2) {
				while(j>=0&&((Comparable)x.getTuples().get(j).getAttributes().get(pos)).compareTo((Comparable)_objValue)==0) {
					res.add(0, x.getTuples().get(j));
				    j--;
				}
			}
		}
		return res;
	}
	private ArrayList<Tuple> goBinary(String _strColumnName, Object _objValue, String _strOperator, int pos) throws DBAppException {
		ArrayList<Tuple> res=new ArrayList();
		switch(_strOperator) {
		case ">":
		case ">=": res=mtOrMtlBinary( _strColumnName,  _objValue, _strOperator,pos);break;
		case "<":
		case "<=":res=ltOrLtlBinary(_strColumnName,  _objValue, _strOperator,pos);break;
		case "=":res=equalsBinary(_strColumnName,  _objValue, _strOperator,pos);break;
		}
		return res;
		
	}
	private ArrayList<Tuple> equalsBinary(String _strColumnName, Object _objValue, String _strOperator, int pos) throws DBAppException{
		
		
		String[] searchResult = SearchInTable(tableName, _objValue).split("#");
		String startPage = searchResult[0];
		int startPageIndex = getPageIndex(startPage);
		int startTupleIndex = Integer.parseInt(searchResult[1]); 
		ArrayList<Tuple> res = new ArrayList<>();
		for (int pageIdx = startPageIndex ,tupleIdx = startTupleIndex; pageIdx<pages.size(); pageIdx++,tupleIdx=0) {
			if(((Comparable) min.get(pageIdx)).compareTo((Comparable)_objValue)>0)break;
			Page currentPage = deserialize(pages.get(pageIdx)); 
			while (tupleIdx<currentPage.getTuples().size()  &&	((Comparable)currentPage.getTuples().get(tupleIdx).getAttributes().get(pos)).compareTo(_objValue)==0 )
				res.add(currentPage.getTuples().get(tupleIdx++));			
		}
		
		return res;
	}
	
	private ArrayList<Tuple> ltOrLtlBinary(String _strColumnName, Object _objValue, String _strOperator, int pos) throws DBAppException {
		return ltOrLtlLinear(_strColumnName, _objValue, _strOperator, pos);
	}
	private ArrayList<Tuple> mtOrMtlBinary(String _strColumnName, Object _objValue, String _strOperator, int pos) throws DBAppException{
		return mtOrMtlLinear(_strColumnName, _objValue, _strOperator, pos);
		
	}
	private ArrayList<Tuple> goWithIndex(String _strColumnName, Object _objValue, String _strOperator, int pos) throws DBAppException {
		// TODO Auto-generated method stub
		ArrayList<Tuple> res=new ArrayList();
		switch(_strOperator) {
		case ">":
		case ">=":res= mtOrMtlIndex( _strColumnName,  _objValue, _strOperator,pos);break;
		case "<":
		case "<=":res=ltOrLtlIndex(_strColumnName,  _objValue, _strOperator,pos);break;
		case "=":res=equalsIndex(_strColumnName,  _objValue, _strOperator,pos);break;
		}
		return res;
	}
	private ArrayList<Tuple> equalsIndex(String _strColumnName, Object _objValue, String _strOperator, int pos) {
		// TODO Auto-generated method stub
		return null;
	}
	private ArrayList<Tuple> ltOrLtlIndex(String _strColumnName, Object _objValue, String _strOperator, int pos) throws DBAppException {
		if(_strColumnName.equals(strClusteringKey))
			return ltOrLtlLinear(_strColumnName, _objValue, _strOperator, pos);
		ArrayList<Tuple> res=new ArrayList();
		String lastPage=pages.get(pages.size()-1);
		int lastPageMaxNum=Integer.parseInt(lastPage.substring(tableName.length()));
		boolean []visited=new boolean[lastPageMaxNum];
		BPTree b=colNameBTreeIndex.get(_strColumnName);
		BPTreeLeafNode leaf=b.getLeftmostLeaf();
		while(leaf.getNext()!=null) {
			int i;
		   for( i=0;i<leaf.getNumberOfKeys();i++) {
			   GeneralReference gr=leaf.getRecord(i);
			   if(leaf.getKey(i).compareTo((Comparable)_objValue)>0)break;
			   if(leaf.getKey(i).compareTo((Comparable)_objValue)==0&&_strOperator.length()==1)break;
			   Set<Ref> ref=fillInRef(gr);
			   for(Ref r:ref) {
				   String pagename=r.getPage();
				   int curPageNum=Integer.parseInt(pagename.substring(tableName.length()));
				   if(visited[curPageNum])continue;
				   res=addToResultSet(res,pagename,pos,_objValue,_strOperator);
				   visited[curPageNum]=true;
			   }
			   
		   }
		   if(i<leaf.getNumberOfKeys())break;
		   leaf=leaf.getNext();
		}
		return res;
	}
 	private ArrayList<Tuple> addToResultSet(ArrayList<Tuple> res, String pagename, int pos, Object _objValue,
			String _strOperator) throws DBAppException {
		Page x=deserialize(pagename);
		for(int i=0;i<x.getTuples().size();i++) {
			if(((Comparable)_objValue).compareTo((Comparable)x.getTuples().get(i).getAttributes().get(pos))>=0)res.add(x.getTuples().get(i));
			else if(((Comparable)_objValue).compareTo((Comparable)x.getTuples().get(i).getAttributes().get(pos))==0) {
				if(_strOperator.length()==1)break;
				else res.add(x.getTuples().get(i));
			}
			else break;
		}
		return res;
	}
	private Set<Ref> fillInRef(GeneralReference gr) throws DBAppException {
 		Set<Ref> ref=new HashSet();
 		if(gr instanceof Ref)
			   ref.add((Ref)gr);
		   else {
			   OverflowReference ov=(OverflowReference)gr;
			   OverflowPage ovp=ov.getFirstPage();
			   while(ovp!=null) {
				   for(Ref r:ovp.getRefs())
				       ref.add(r);
				   ovp=ovp.getNext1();
//TODO: IMPORTANT :::: HashCode in Class REF so that the hashset remains a SET
				   //Eslam wrote the previous comment

			   }
		   }
 		return ref;
	}
	private ArrayList<Tuple> mtOrMtlIndex(String _strColumnName, Object _objValue, String _strOperator, int pos) throws DBAppException {
 		if(_strColumnName.equals(strClusteringKey))
			return mtOrMtlLinear(_strColumnName, _objValue, _strOperator, pos);
		//TODO non cluster
		return null;
	}
	public void checkQueryValidity(SQLTerm[] arrSQLTerms,Vector<String[]> metaOfTable) throws DBAppException{
	//	boolean clusterHasIndex=false;
		for(SQLTerm x:arrSQLTerms) {
			 int i;
			 for( i=0;i<metaOfTable.size();i++) {
				 if(metaOfTable.get(i)[0].equals(tableName) &&
						 metaOfTable.get(i)[1].equals(x._strColumnName)) {
					 try {
					    Class colType = Class.forName(metaOfTable.get(i)[2]);

						Class parameterType = x._objValue.getClass();
						Class polyOriginal = Class.forName("java.awt.Polygon");
						if (colType == polyOriginal) {
							colType = Class.forName("kalabalaDB.Polygons");
						}
						if (!colType.equals(parameterType)) {
							throw new DBAppException("DATA types 8alat");

						}}
					   catch(ClassNotFoundException e) {
						   throw new DBAppException("Class Not Found Exception");
					   }
					// if(metaOfTable.get(i)[3].equals("True")&&metaOfTable.get(i)[3].equals("True"))
						// clusterHasIndex=true;
				 }
			 
			 }
			 if(i==metaOfTable.size()) throw new DBAppException("Column "+x._strColumnName+" doesn't exist");
		 }
		//return clusterHasIndex;
	}

	public static Comparable parseObject(String strTableName, Object strKey) throws DBAppException{
		try {
			Vector meta = DBApp.readFile("data/metadata.csv");
			Comparable key = null;
			for (Object O : meta) {
				String[] curr = (String[]) O;
				if (curr[0].equals(strTableName) && curr[3].equals("True")) // search in metadata for the table name and the
																			// key
				{
					if (curr[2].equals("java.lang.Integer"))
						key = (Integer)(strKey);
					else if (curr[2].equals("java.lang.Double"))
						key = (Double)(strKey);
					else if (curr[2].equals("java.util.Date"))
						key = (Date)(strKey);
					else if (curr[2].equals("java.lang.Boolean"))
						key = (Boolean)(strKey);
					else if (curr[2].equals("java.awt.Polygon"))
						key = (Polygons) strKey;
					else {
						throw new DBAppException("Searching for a key of unknown type !");
					}
				}
			}
			return key;
		}
		catch(ClassCastException e) {
			throw new DBAppException("Class Cast Exception");
		}
	}

	public static Comparable parseString(String strTableName, String  strKey) throws DBAppException{
		try {
			Vector meta = DBApp.readFile("data/metadata.csv");
			Comparable key = null;
			for (Object O : meta) {
				String[] curr = (String[]) O;
				if (curr[0].equals(strTableName) && curr[3].equals("True")) // search in metadata for the table name and the
																			// key
				{
					if (curr[2].equals("java.lang.Integer"))
						key = Integer.parseInt(strKey);
					else if (curr[2].equals("java.lang.Double"))
						key = Double.parseDouble(strKey);
					else if (curr[2].equals("java.util.Date"))
						key = Date.parse(strKey);
					else if (curr[2].equals("java.lang.Boolean"))
						key = Boolean.parseBoolean(strKey);
					else if (curr[2].equals("java.awt.Polygon"))
						key = (Comparable) Polygons.parsePolygon(strKey);
					else {
//						TODO:return "-1";
						throw new DBAppException("Searching for a key of unknown type !");
					}
				}
			}
			return key;
		}
		catch(ClassCastException e) {
			throw new DBAppException("Class Cast Exception");
		}
	}

	public String SearchInTable(String strTableName, Object strKey) throws DBAppException {
		return SearchInTable(strTableName, parseObject(strTableName, strKey));
	}
	
	public String SearchInTable(String strTableName, String strKey) throws DBAppException {
		return SearchInTable(strTableName, parseString(strTableName, strKey));
	}
	
	public String SearchInTable(String strTableName, Comparable key) throws DBAppException {
		try {
			
			Table t = this;
			Vector<String> pages = t.getPages();
			// Vector<String> MinMax = t.getMin().toString() ;
	
			for (String s : pages) {
				Page p = Table.deserialize(s);
				int l = 0;
				int r = p.getTuples().size()-1;
	
				while (l <= r) {
					int m = l + (r - l) / 2;
	
					// Check if x is present at mid
					if (key.equals((p.getTuples().get(m)).getAttributes().get(t.getPrimaryPos()))) {
						while (m > 0 && key.equals((p.getTuples().get(m - 1)).getAttributes().get(t.getPrimaryPos()))) {
							m--;
						}
						return p.getPageName() + "#" + m;
					}
	
					// If x greater, ignore left half
					if (key.compareTo((p.getTuples().get(m)).getAttributes().get(t.getPrimaryPos())) < 0)
						r = m - 1;
	
					// If x is smaller, ignore right half
					else
						l = m + 1;
				}
//				p.serialize(); // added by abdo
			}
//			serialize(t); // addd by abdo
	
//			return "-1";
			throw new DBAppException("Searched for a tuple that does not exist in the table");
		}
		catch(ClassCastException e) {
			throw new DBAppException("Class Cast Exception");
		}
	}
	
			

	public String toString() {
		StringBuilder sb = new StringBuilder();
		
		sb.append("Name: "+tableName+"\n");
		
		sb.append("Clustering key: "+strClusteringKey+" @ pos="+primaryPos+"\n");
		
		sb.append("Pages:\n{");
		for (int i=0;i<pages.size()-1;i++) {
			sb.append(pages.get(i)+", ");
		}
		if (pages.size()>0) sb.append(pages.get(pages.size()-1)+"}\n");
		
		sb.append("Min:\n{");
		for (int i=0;i<min.size()-1;i++) {
			sb.append(min.get(i)+", ");
		}
		if (min.size()>0) sb.append(min.get(min.size()-1)+"}\n");
		
		sb.append("Max:\n{");
		for (int i=0;i<max.size()-1;i++) {
			sb.append(max.get(i)+", ");
		}
		if (max.size()>0) sb.append(max.get(max.size()-1)+"}\n");
		
		sb.append("Indexed Columns: \n");
		for (String col:colNameBTreeIndex.keySet()) {
			sb.append(col+"\t");
		}
		
		return sb.toString();
	}
	
	public int getPageIndex(String pageName) {
		int pageOriginalNum = getSuffix(pageName);
		int i=pageOriginalNum;
		for (;i>=0 && getSuffix(pages.get(i))>pageOriginalNum;i--);
		return i;
	}
	public int getSuffix(String pageName) {
		return Integer.parseInt(pageName.substring(tableName.length()));
	}
/*
	public static void main(String[] args) {
		ArrayList<Tuple> arr1 = new ArrayList<>();
		ArrayList<Tuple> arr2 = new ArrayList<>();
		int n = (int)(1+Math.random()*5);
		int m = (int)(1+Math.random()*5);
		for (int i=0;i<n;i++) {
			Tuple t = new Tuple();
			t.addAttribute((int)(Math.random()*9));
			arr1.add(t);
		}
		for(int i=0;i<m;i++) {
			Tuple t = new Tuple();
			t.addAttribute((int)(Math.random()*9));
			arr2.add(t);
		}
		System.out.println(show(arr1));
		System.out.println(show(arr2));
		Table k = new Table();
		ArrayList<Tuple> and = k.andSets(arr1, arr2);
		ArrayList<Tuple> or = k.orSets(arr1, arr2);
		ArrayList<Tuple> xor = k.xorSets(arr1, arr2);
		System.out.println("And="+show(and));
		System.out.println("Or="+show(or));
		System.out.println("Xor="+show(xor));
		
	}
*/
	static void tstGettingPageIndexFromName(String[] args) {
		Table t = new Table();
		t.setTableName("Tab");
		Vector<String> pg = t.pages;
		for (int i=0;i<20;i++) {
			pg.add("Tab"+i);
		}
		for (int i=0;i<20;i++) {
			System.out.printf("indx of Tab%d=%d\n",i,t.getPageIndex("Tab"+i));
		}
		int i=0;
		pg.remove(10);
		pg.remove(10);
		pg.remove(5);
		for (i=0;i<18;i++) {
			System.out.printf("indx of Tab%d=%d\n",i,t.getPageIndex("Tab"+i));
			System.out.println(pg.get(t.getPageIndex("Tab"+i)));
		}
	}
	static String show(ArrayList<Tuple> arr) {
		StringBuilder sb = new StringBuilder();
		sb.append("{");
		for (int i=0;i<arr.size()-1;i++) {
			sb.append(arr.get(i).getAttributes().get(0)+", ");
		}
		if (arr.size()>0)
			sb.append(arr.get(arr.size()-1).getAttributes().get(0));
		sb.append("}");
		return sb.toString();
	}
}
