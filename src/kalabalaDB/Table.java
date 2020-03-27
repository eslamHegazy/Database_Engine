package kalabalaDB;
import java.io.*;
import java.util.*;

import BPTree.BPTree;
import BPTree.Ref;




public class Table implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1273622720148360313L;
	private Vector<String> pages = new Vector();
	private int MaximumRowsCountinPage;
	private Vector<Object>min=new Vector<>();
	private Vector<Object>max=new Vector<>();
	private String tableName;
	private String strClusteringKey;
	private int primaryPos;
	private Hashtable<String,BPTree> colNameBTreeIndex= new Hashtable<>();
	
	public Hashtable<String, BPTree> getColNameBTreeIndex() {
		return colNameBTreeIndex;
	}
	public  Ref searchWithCluster(Comparable key,BPTree b) throws DBAppException {
		Ref ref=b.searchRequiredReference(key); //NOT IMPLEMENTED
		if(ref==null) { //returns null if key is the least value in tree
			return new Ref(Integer.parseInt(pages.get(0).substring(tableName.length())));
		}
		return ref;
	}
	public void printIndices() {
		for (String x: colNameBTreeIndex.keySet()) {
			BPTree  b = colNameBTreeIndex.get(x);
			System.out.println(x);
			System.out.println(b);
			System.out.println();
		}
	}
	public Vector<String> getPages() {
		return pages;
	}
	public String getNewPageName() {
		return tableName+((pages.size()==0)?0:Integer.parseInt((pages.get(pages.size()-1)).substring(tableName.length()))+1);
		
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
			FileInputStream fileIn = new FileInputStream("data/"+name + ".class");
	//		FileInputStream fileIn = new FileInputStream("data/"+name + ".ser");
			//TODO: Check resulting path + check class/ ser
			ObjectInputStream in = new ObjectInputStream(fileIn);
			Page xx = (Page) in.readObject();
			in.close();
			fileIn.close();
			return xx;
		}
		catch(IOException e) {
			throw new DBAppException("IO Exception");
		}
		catch(ClassNotFoundException e) {
			throw new DBAppException("Class Not Found Exception");
		}
	}

	public static void addInVector(Vector<Object> vs, Object str, int n) {
		vs.insertElementAt(str, n);
	}

	public void addInPage(int curr, Tuple x,String keyType,String keyColName,int nodeSize) throws DBAppException, IOException {
//		System.out.println(x+" "+curr);
		if (curr < pages.size()) {
			String pageName = pages.get(curr);

			Page p = deserialize(pageName);
			if (p.size() < MaximumRowsCountinPage) {
//				System.out.println("blboz2");
				p.insertIntoPage(x, primaryPos);
//				System.out.println("blboz3");
				Object minn = p.getTuples().get(0).getAttributes().get(primaryPos);
				Object maxx = p.getTuples().get(p.size() - 1).getAttributes().get(primaryPos);
				min.remove(curr);
				addInVector(min, minn, curr);
				max.remove(curr);
				addInVector(max, maxx, curr);
				if(colNameBTreeIndex.containsKey(keyColName)){
					BPTree bTree=colNameBTreeIndex.get(keyColName);
					int index=getIndexNumber(p.getPageName(),tableName.length());
					Ref recordReference = new Ref(index);
					bTree.insert((Comparable) x.getAttributes().get(primaryPos), recordReference);
					colNameBTreeIndex.put(keyColName,bTree);
				}
				p.serialize();
			} else {
				// Tuple t=p.getTuples().get(p.size()-1);//element 199
				p.insertIntoPage(x, primaryPos);
				if(colNameBTreeIndex.containsKey(keyColName)){
					BPTree bTree=colNameBTreeIndex.get(keyColName);
					int index=getIndexNumber(p.getPageName(),tableName.length());
					Ref recordReference = new Ref(index);
					bTree.insert((Comparable) x.getAttributes().get(primaryPos), recordReference);
					colNameBTreeIndex.put(keyColName,bTree);
				}
				Tuple t = p.getTuples().remove(p.size() - 1);
				if(colNameBTreeIndex.containsKey(keyColName)){
					BPTree bTree=colNameBTreeIndex.get(keyColName);
					bTree.delete((Comparable) t.getAttributes().get(primaryPos));
				}
				Object minn = p.getTuples().get(0).getAttributes().get(primaryPos);
				Object maxx = p.getTuples().get(p.size() - 1).getAttributes().get(primaryPos);
				min.remove(curr);
				addInVector(min, minn, curr);
				max.remove(curr);
				addInVector(max, maxx, curr);
				
				p.serialize();
				addInPage(curr + 1, t,keyType,keyColName,nodeSize);
			}
		} else {
			Page p = new Page(getNewPageName());
			p.insertIntoPage(x, primaryPos);
			if(colNameBTreeIndex.containsKey(keyColName)){
				BPTree bTree=colNameBTreeIndex.get(keyColName);
				int index=getIndexNumber(p.getPageName(),tableName.length());
				Ref recordReference = new Ref(index);
				bTree.insert((Comparable) x.getAttributes().get(primaryPos), recordReference);
				colNameBTreeIndex.put(keyColName,bTree);
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
			if(colNameBTreeIndex.containsKey(keyColName)){
				BPTree bTree=colNameBTreeIndex.get(keyColName);
				int index=getIndexNumber(p.getPageName(),tableName.length());
				Ref recordReference = new Ref(index);
				bTree.insert((Comparable) x.getAttributes().get(primaryPos), recordReference);
				colNameBTreeIndex.put(keyColName,bTree);
			}
			pages.addElement(p.getPageName());
			min.addElement(keyV);
			max.addElement(keyV);	
			p.serialize();

		}else{
			Comparable keyValue=(Comparable) keyV;
			if(colNameBTreeIndex.containsKey(keyColName)){
				BPTree tree=colNameBTreeIndex.get(keyColName);
				Ref pageReference=tree.searchForInsertion(keyValue);
				String pageName=this.tableName+pageReference.getPage();
				int curr=pages.indexOf(pageName);
				addInPage(curr,x,keyType,keyColName,nodeSize);
			}else{
				int lower = 0;
				int upper = min.size();
				int curr=0;
				for(curr=0;curr<pages.size();curr++){
					Object minn=(min.get(curr));
					Object maxx=max.get(curr);
					if((keyValue.compareTo(minn)>=0&&keyValue.compareTo(maxx)<=0)||(keyValue.compareTo(minn)<0)||curr==pages.size()-1){
						addInPage(curr, x,keyType,keyColName,nodeSize);
						break;
					}
				}
			}
		}

		Set<String> c=colNameBTreeIndex.keySet();
		for(int i=0;i<c.size();i++){
			if(!keyColName.equals(c)){
				BPTree tree=colNameBTreeIndex.get(c);
				int index=0;
				for(;index<colNames.size();index++){
					if(keyColName.equals(colNames.get(index))){
						break;
					}
				}
				Object keyValueOfNonCluster=x.getAttributes().get(index);
				Ref pageReference=tree.searchForInsertion((Comparable) keyValueOfNonCluster);
				tree.insert((Comparable) keyValueOfNonCluster, pageReference);
			}
		}
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
					System.out.println(colType+" "+parameterType);
					if (!colType.equals(parameterType))
						return true;
					else
						break;
					}
					catch(ClassNotFoundException e) {
						throw new DBAppException("Class Not Found Exception");
					}
				}
			}
			if (i < metaOfTable.size())
				return false;
		}
		return true;
	}

	/*
	 * public void deleteInTable(Object keyValue, String pageName, int tuplePos) {
	 * try { Page p = deserialize(pageName); FileInputStream fileIn = new
	 * FileInputStream(pageName + ".ser"); ObjectInputStream in = new
	 * ObjectInputStream(fileIn); Page xx = (Page) in.readObject(); in.close();
	 * fileIn.close(); p.deleteInPage(keyValue, tuplePos); String newP = ""; int
	 * newPPos = 0; if (p.size() == 0) { File f = new File(pageName + ".ser");
	 * f.delete(); for (int i = 0; i < pages.size(); i++) { if
	 * (pages.get(i).equals(pageName)) { MinMax.remove(i); pages.remove(i); newP =
	 * pages.get(i + 1); newPPos = i + 1; } } seeAnother(keyValue, newP, newPPos);
	 * 
	 * } else { Tuple t0 = p.getTuples().get(0); Tuple tn =
	 * p.getTuples().get(p.size() - 1); String k0 =
	 * (t0.getAttributes().get(primaryPos)).toString(); String kn =
	 * (tn.getAttributes().get(primaryPos)).toString(); String minMax = k0 + "," +
	 * kn; for (int i = 0; i < pages.size(); i++) { if
	 * (pages.get(i).equals(pageName)) { MinMax.setElementAt(minMax, i); newP =
	 * pages.get(i + 1); newPPos = i + 1; } } seeAnother(keyValue, newP, newPPos);
	 * 
	 * } } catch (ClassNotFoundException | IOException e) { // TODO Auto-generated
	 * catch block e.printStackTrace(); }
	 * 
	 * }
	 */
	public void deleteInTable(Hashtable<String, Object> htblColNameValue, Vector<String[]> metaOfTable)
			throws DBAppException {

		if (invalidDelete(htblColNameValue, metaOfTable)) {
			throw new DBAppException("false operation");
			//TODO: Is this message appropriate?
		}
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
				FileInputStream fileIn = new FileInputStream("data/"+pageName + ".class");
				//TODO: Check resulting path
				ObjectInputStream in = new ObjectInputStream(fileIn);
				Page xx = (Page) in.readObject();
				in.close();
				fileIn.close();
			}
			catch(ClassNotFoundException e) {
				throw new DBAppException("Class Not Found Exception");
			}
			catch(IOException e) {
				throw new DBAppException("IO Exception");
			}
			p.deleteInPage(htblColNameValue,attributeIndex);
			if(p.getTuples().size()==0)
			{
				File f = new File("data/"+pageName + ".class");
				//TODO: Check resulting path
				f.delete();
				pages.remove(i);
				min.remove(i);
				max.remove(i);
				i--;
				
			}
			else
			{
				Object minn= p.getTuples().get(0).getAttributes().get(primaryPos);
				Object maxx= p.getTuples().get(p.size()-1).getAttributes().get(primaryPos);
				min.setElementAt(minn, i);
				max.setElementAt(maxx, i);
				p.serialize();
			}
		}

	}
	
	
	public void createBTreeIndex(String strColName,BPTree bTree,int colPosition) throws DBAppException, IOException{
		if(colNameBTreeIndex.containsKey(strColName)){
			throw new DBAppException("BTree index already exists on this column");
		}else{
			colNameBTreeIndex.put(strColName,bTree);
		}
		for(String str:pages){
			Page p=deserialize(str);
			int index=getIndexNumber(p.getPageName(),tableName.length());
			int i=0;
			for(Tuple t:p.getTuples()){
				Ref recordReference = new Ref(index);
				bTree.insert((Comparable) t.getAttributes().get(colPosition), recordReference);
				i++;
			}
			p.serialize();
		}
	}
	public static int getIndexNumber(String pName,int s){
		String num=pName.substring(s);
		return Integer.parseInt(num);
	}
	public Iterator<Tuple> selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators,Vector<String[]> metaOfTable) throws DBAppException {
		
		Iterator<Tuple> s=inspectCols(arrSQLTerms,strarrOperators,metaOfTable);//method inspect cols determines which way to search through pages
		if(s!=null)
			return s;
		
		
		return s;
	}
	
	public Iterator<Tuple> inspectCols(SQLTerm[] arrSQLTerms, String[] strarrOperators,Vector<String[]> metaOfTable) throws DBAppException{
		 //check for colnames or validity type .. returns true if clustering key has an index
			boolean clusterHasIndex= checkQueryValidity(arrSQLTerms,metaOfTable);
			
		 //safe to check for other indices
		String s=(clusterHasIndex)?strClusteringKey:getFirstIndexedCol(arrSQLTerms);
		if(s.equals("-1")) //no indices at all
			return null;
		 
		//now we choose one index which is s
		
		 return null; //to be changed
	}
	public String getFirstIndexedCol(SQLTerm[] arrSQLTerms) throws DBAppException{
	       for(SQLTerm x:arrSQLTerms) {
	    	   if(colNameBTreeIndex.containsKey(x._strColumnName))
	    		   return x._strColumnName;
	       }
	       return "-1";
	}
	public boolean checkQueryValidity(SQLTerm[] arrSQLTerms,Vector<String[]> metaOfTable) throws DBAppException{
		boolean clusterHasIndex=false;
		for(SQLTerm x:arrSQLTerms) {
			 int i;
			 for( i=0;i<metaOfTable.size();i++) {
				 if(metaOfTable.get(i)[1].equals(x._strColumnName)) {
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
					 if(metaOfTable.get(i)[3].equals("True")&&metaOfTable.get(i)[3].equals("True"))
						 clusterHasIndex=true;
				 }
			 }
			 if(i==metaOfTable.size()) throw new DBAppException("Column "+x._strColumnName+" doesn't exist");
		 }
		return clusterHasIndex;
	}
	/*public void seeAnother(Object keyValue, String pageName, int newPPos) {
		try {
			while (newPPos < pages.size()) {
				Page p = deserialize(pageName);
				FileInputStream fileIn = new FileInputStream(pageName + ".ser");
				ObjectInputStream in = new ObjectInputStream(fileIn);
				Page xx = (Page) in.readObject();
				in.close();
				fileIn.close();
				p.deleteInPage2(keyValue, primaryPos);
				if (p.size() == 0) {
					File f = new File(pageName + ".ser");
					f.delete();
					for (int i = 0; i < pages.size(); i++) {
						if (pages.get(i).equals(pageName)) {
							MinMax.remove(i);
							pages.remove(i);
						}
					}
				} else {
					Tuple t0 = p.getTuples().get(0);
					Tuple tn = p.getTuples().get(p.size() - 1);
					String k0 = (t0.getAttributes().get(primaryPos)).toString();
					String kn = (tn.getAttributes().get(primaryPos)).toString();
					String minMax = k0 + "," + kn;
					for (int i = 0; i < pages.size(); i++) {
						if (pages.get(i).equals(pageName)) {
							MinMax.setElementAt(minMax, i);
						}
					}
				}
				newPPos++;
			}
		} catch (ClassNotFoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}*/

	/*
	 * public void deleteInTable(Object keyValue ) { int lower = 0; int upper =
	 * MinMax.size(); while (lower < upper) {
	 * 
	 * int curr = (lower + upper) / 2; String mM = MinMax.get(curr); String[] inter;
	 * // add in which page and new lower and upper String[] MM = mM.split(",");
	 * if(keyValue.toString().compareTo(MM[0])>=0 &&
	 * keyValue.toString().compareTo(MM[1])<=0){ String pageName = pages.get(curr);
	 * try { Page p = deserialize(pageName); FileInputStream fileIn = new
	 * FileInputStream(pageName + ".ser"); ObjectInputStream in = new
	 * ObjectInputStream(fileIn); Page xx = (Page) in.readObject(); in.close();
	 * fileIn.close(); p.deleteInPage(keyValue,primaryPos); if(p.size()==0) { File f
	 * = new File(pageName + ".ser"); f.delete(); MinMax.remove(curr);
	 * pages.remove(curr); } } catch (ClassNotFoundException | IOException e) { //
	 * TODO Auto-generated catch block e.printStackTrace(); } } }
	 * 
	 * 
	 * }
	 */

//	private void writeObject(ObjectOutputStream out) throws IOException{
//		out.writeObject(pages);
//		out.writeObject(MaximumRowsCountinPage);
//		out.writeObject(min);
//		out.writeObject(max);
//		out.writeObject(tableName);
//		out.writeObject(strClusteringKey);
//		out.writeObject(primaryPos);
//		out.writeObject(new HashtableSerializer(colNameBTreeIndex));
//	}
//	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException{
//		this.pages=(Vector<String>) in.readObject();
//		this.MaximumRowsCountinPage= (int) in.readObject();
//		this.min=(Vector<Object>) in.readObject();
//		this.max=(Vector<Object>) in.readObject();
//		this.tableName=(String) in.readObject();
//		this.strClusteringKey=(String) in.readObject();
//		this.primaryPos=(int) in.readObject();
//		this.colNameBTreeIndex = ((HashtableSerializer)(in.readObject())).getHashtable();
//	}
	
	
}
