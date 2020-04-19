package kalabalaDB;

import java.awt.Polygon;
import java.io.*;
import java.util.*;

import BPTree.BPTree;
import BPTree.BPTreeLeafNode;
import General.GeneralReference;
import General.LeafNode;
import General.OverflowPage;
import General.OverflowReference;
import General.Ref;
import General.TreeIndex;
import RTree.RTree;

public class Table implements Serializable {

	private Vector<String> pages = new Vector<>();
	private int MaximumRowsCountinPage;
	//private Vector<Object> min = new Vector<>();
	//private Vector<Object> max = new Vector<>();
	private String tableName;
	private String strClusteringKey;
	private int primaryPos;
	private int lastID;
	private Hashtable<String, TreeIndex> colNameTreeIndex = new Hashtable<>();

	public int getLastID(boolean increment) {
		if (increment)
			return lastID++;
		return lastID;
	}

	public Hashtable<String, TreeIndex> getColNameBTreeIndex() {
		return colNameTreeIndex;
	}

	public Ref searchWithCluster(Comparable key, BPTree b) throws DBAppException {
		Ref ref = b.searchRequiredReference(key); // NOT IMPLEMENTED
		if (ref == null) { // returns null if key is the least value in tree
			return new Ref(pages.get(0));
		}
		return ref;
	}

	public void printIndices() {
		for (String x : colNameTreeIndex.keySet()) {
			TreeIndex b = colNameTreeIndex.get(x);
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

	

	public Comparable getMax(int index) throws DBAppException
	{
		String pageName = pages.get(index);
		Page p = deserialize(pageName);
		Object maxx = p.getTuples().get(p.size() - 1).getAttributes().get(primaryPos);
		return (Comparable)maxx;
	}

	public Comparable getMin(int index) throws DBAppException
	{
		String pageName = pages.get(index);
		Page p = deserialize(pageName);
		Comparable minn = (Comparable)p.getTuples().get(0).getAttributes().get(primaryPos);
		return minn;
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
			System.out.println("IO||||\t deserialize:page:" + name);
			FileInputStream fileIn = new FileInputStream("data/" + name + ".class");
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

	public Hashtable<Tuple, String> addInPage(int curr, Tuple x, String keyType, String keyColName, int nodeSize,
			boolean doInsert, Hashtable<Tuple, String> list) throws DBAppException, IOException {
		// System.out.println(x+" "+curr);\

		if (curr < pages.size()) {
			String pageName = pages.get(curr);

			Page p = deserialize(pageName);
			if (p.size() < MaximumRowsCountinPage) {
				// System.out.println("blboz2");
				p.insertIntoPage(x, primaryPos);
				// System.out.println("blboz3");

				/*
				 * Object minn = p.getTuples().get(0).getAttributes().get(primaryPos); Object
				 * maxx = p.getTuples().get(p.size() - 1).getAttributes().get(primaryPos);
				 * min.remove(curr); addInVector(min, minn, curr); max.remove(curr);
				 * addInVector(max, maxx, curr);
				 */
				if (colNameTreeIndex.containsKey(keyColName) && doInsert) {
					TreeIndex tree = colNameTreeIndex.get(keyColName);
					Ref recordReference = new Ref(p.getPageName());
					tree.insert((Comparable) x.getAttributes().get(primaryPos), recordReference);
					// System.out.println(tree);
				}
				p.serialize();
				return list;
			} else {
				// Tuple t=p.getTuples().get(p.size()-1);//element 199
				p.insertIntoPage(x, primaryPos);
				if (colNameTreeIndex.containsKey(keyColName) && doInsert) {
					TreeIndex tree = colNameTreeIndex.get(keyColName);
					Ref recordReference = new Ref(p.getPageName());
					tree.insert((Comparable) x.getAttributes().get(primaryPos), recordReference);
					// System.out.println(tree);
				}
				Tuple t = p.getTuples().remove(p.size() - 1);
				if (colNameTreeIndex.containsKey(keyColName)) {

					TreeIndex tree = colNameTreeIndex.get(keyColName);
					String newp = "";
					if (curr + 1 < pages.size()) {
						newp = pages.get(curr + 1);
					} else {
						Page n = new Page(getNewPageName());
						pages.addElement(n.getPageName());
						newp = n.getPageName();
						Object keyValue = t.getAttributes().get(primaryPos);
						//min.addElement(keyValue);
						//max.addElement(keyValue);
						n.serialize();
					}
					// System.out.println("change ref "+p.getPageName()+" "+newp+" "+t);
					tree.updateRef(p.getPageName(), newp, (Comparable) t.getAttributes().get(primaryPos),
							tableName.length());
					// System.out.println(t+" "+newp);
					// System.out.println("changed ref
					// "+getClusterReference(t.getAttributes().get(primaryPos), keyColName));
				}
				list.put(t, p.getPageName());
				Object minn = p.getTuples().get(0).getAttributes().get(primaryPos);
				Object maxx = p.getTuples().get(p.size() - 1).getAttributes().get(primaryPos);
				//min.remove(curr);
				//addInVector(min, minn, curr);
				//max.remove(curr);
				//addInVector(max, maxx, curr);

				p.serialize();
				// System.out.println(Arrays.asList(list.keySet()));
				return addInPage(curr + 1, t, keyType, keyColName, nodeSize, false, list);
			}
		} else {
			Page p = new Page(getNewPageName());
			p.insertIntoPage(x, primaryPos);
			if (colNameTreeIndex.containsKey(keyColName)) {
				TreeIndex tree = colNameTreeIndex.get(keyColName);
				Ref recordReference = new Ref(p.getPageName());
				tree.insert((Comparable) x.getAttributes().get(primaryPos), recordReference);
			}
			Object keyValue = p.getTuples().get(0).getAttributes().get(primaryPos);
			pages.addElement(p.getPageName());
			//min.addElement(keyValue);
			//max.addElement(keyValue);
			p.serialize();
			return list;
		}

	}

	// public static void createBPTreeGivinType(BPTree bTree,String colType,int
	// nodeSize) throws DBAppException{
	// switch(colType){
	// case "java.lang.Integer":bTree=new BPTree<Integer>(nodeSize);break;
	// case "java.lang.Double":bTree=new BPTree<Double>(nodeSize);break;
	// case "java.util.Date":bTree=new BPTree<Date>(nodeSize);break;
	// case "java.lang.Boolean":bTree=new BPTree<Boolean>(nodeSize);break;
	// case "java.awt.Polygon
	// ":bTree=new BPTree<Polygons>(nodeSize);break;
	// default :throw new DBAppException("I've never seen this colType in my life");
	// }
	// }

	public void insertSorted(Tuple x, Object keyV, String keyType, String keyColName, int nodeSize, ArrayList colNames)
			throws DBAppException, IOException {
		Hashtable<Tuple, String> list = new Hashtable<Tuple, String>();
		if (pages.size() == 0) {
			Page p = new Page(getNewPageName());
			p.insertIntoPage(x, primaryPos);
			if (colNameTreeIndex.containsKey(keyColName)) {
				TreeIndex tree = colNameTreeIndex.get(keyColName);
				Ref recordReference = new Ref(p.getPageName());
				tree.insert((Comparable) x.getAttributes().get(primaryPos), recordReference);
			}
			pages.addElement(p.getPageName());
			//min.addElement(keyV);
			//max.addElement(keyV);
			p.serialize();

		} else {
			Comparable keyValue = (Comparable) keyV;
			if (colNameTreeIndex.containsKey(keyColName)) {
				TreeIndex tree = colNameTreeIndex.get(keyColName);
				Ref pageReference = tree.searchForInsertion(keyValue, tableName.length());
				// System.out.println("searchForInsertion "+pageReference + " " + keyValue);
				String pageName = "";
				if (pageReference == null) {
					pageName = pages.get(pages.size() - 1);
				} else
					pageName = pageReference.getPage();
				int curr = pages.indexOf(pageName);
				list = addInPage(curr, x, keyType, keyColName, nodeSize, true, list);
			} else {
				int curr = 0;
				for (curr = 0; curr < pages.size(); curr++) {
					Object minn = (getMin(curr));
					Object maxx = getMax(curr);
					if ((keyValue.compareTo(minn) >= 0 && keyValue.compareTo(maxx) <= 0)
							|| (keyValue.compareTo(minn) < 0) || curr == pages.size() - 1) {
						list = addInPage(curr, x, keyType, keyColName, nodeSize, true, list);
						break;
					}
				}
			}
		}
		Set<Tuple> st = list.keySet();
		Set<String> c = colNameTreeIndex.keySet();
		for (String s : c) {
			if (!keyColName.equals(s)) {
				TreeIndex tree = colNameTreeIndex.get(s);
				int index = 0;
				for (; index < colNames.size(); index++) {
					if (s.equals(colNames.get(index))) {
						break;
					}
				}
				Object keyValueOfNonCluster = x.getAttributes().get(index);
				Ref pageReference = getClusterReference(keyV, keyColName);

				tree.insert((Comparable) keyValueOfNonCluster, pageReference);
				// System.out.println(pageReference.getPage());

				for (Tuple t : st) {
					if (!t.equals(x)) {
						Object keyValueOfNonClusterT = t.getAttributes().get(index);
						// tree.delete((Comparable)keyValueOfNonClusterT,list.get(t));
						// Ref pageReferenceT =
						// getClusterReference(t.getAttributes().get(primaryPos),keyColName);
						// tree.insert((Comparable) keyValueOfNonClusterT, pageReferenceT);
						// System.out.println(t+" "+pageReference+"\n");
						tree.updateRef(list.get(t), pages.get(pages.indexOf(list.get(t)) + 1),
								(Comparable) keyValueOfNonClusterT, tableName.length());
					}
				}
			}
		}
		// System.out.println(Arrays.asList(c));
		// Set<String> c = colNameBTreeIndex.keySet();
		// for (String curColName : c) {
		// if (curColName.equals(keyColName)) continue;
		// BPTree tree = colNameBTreeIndex.get(curColName);
		// if (tree == null ) continue;
		// int index = 0;
		// for (; index < colNames.size() ; index++) {
		//// String currentColumn = (String) colNames.get(index);
		// if (keyColName.equals(colNames.get(index))) {
		// continue;
		// }
		// }
		// Object keyValueOfNonCluster = x.getAttributes().get(index);
		// //TODO: getting NullPointerException here
		//// Ref pageReference = tree.searchForInsertion((Comparable)
		// keyValueOfNonCluster);
		// Ref pageReference = insertedReference; //Update it in the above if-else-if...
		// blocks
		// tree.insert((Comparable) keyValueOfNonCluster, pageReference);
		// }
	}

	private Ref getClusterReference(Object keyV, String keyColName) throws DBAppException {
		Comparable keyValue = (Comparable) keyV;
		Ref ref = null;
		if (colNameTreeIndex.contains(keyColName)) {
			TreeIndex tree = colNameTreeIndex.get(keyColName);
			GeneralReference gref = tree.search(keyValue);
			if (gref instanceof Ref) {
				ref = (Ref) gref;
			} else {
				OverflowReference oref = (OverflowReference) gref;
				ref = oref.getLastRef();
			}
		} else {
			for (int i = pages.size() - 1; i >= 0; i--) {
				if (keyValue.compareTo(getMin(i)) >= 0 && keyValue.compareTo(getMax(i)) <= 0) {
					ref = new Ref(pages.get(i));
					break;
				}
			}
		}
		return ref;
	}

	@SuppressWarnings("unchecked")
	public void deleteInTable(Hashtable<String, Object> htblColNameValue, Vector<String[]> metaOfTable,
			String clusteringKey) throws DBAppException, IOException {

		/*
		 * if (invalidDelete(htblColNameValue, metaOfTable)) { throw new
		 * DBAppException("false operation"); // TODO: Is this message appropriate? }
		 */
		try {

			ArrayList<String> indicesGiven = indicesIHave(htblColNameValue, colNameTreeIndex);
			ArrayList<String> allIndices = allTableIndices(colNameTreeIndex);

			if (!(indicesGiven.size() == 0)) {
				String selectedCol = (clusteringKey != null && clusteringKeyHasIndex(indicesGiven, clusteringKey))
						? clusteringKey
						: indicesGiven.get(0);
				boolean isCluster = clusteringKey != null && selectedCol.equals(strClusteringKey);

				TreeIndex tree = colNameTreeIndex.get(selectedCol);
				GeneralReference pageReference = tree.search((Comparable) htblColNameValue.get(selectedCol));
				if (pageReference == null) {
					System.err.println("Value not found to be deleted");
					return;
				}
				// TODO: Eslam: if tried to delete nonexisting value ; null pointer exception ?
				// should we handle ?
				if (pageReference instanceof Ref) {
					Ref x = (Ref) pageReference;
					Page p = deserialize(x.getPage() + "");
					p.deleteInPageforRef(metaOfTable, primaryPos, selectedCol, colNameTreeIndex, htblColNameValue,
							allIndices, isCluster);
					setMinMax(p);

				} else {
					OverflowReference x = (OverflowReference) pageReference;
					OverflowPage OFP = x.getFirstPage();
					Set<Ref> allReferences = getRefFromBPTree(OFP);
					System.out.println(allReferences);
					// OFP.serialize();
					for (Ref ref : allReferences) {
						if (ref != null) {
							System.out.println(ref.getPage());
							Page p = deserialize(ref.getPage() + "");
							// System.out.println(ref.getPage());
							p.deleteInPageforRef(metaOfTable, primaryPos, selectedCol, colNameTreeIndex,
									htblColNameValue, allIndices, isCluster);
							setMinMax(p);
						}

					}

				}

			} else if (clusteringKey != null) {

				for (int i = 0; i < pages.size(); i++) {
					// System.out.println(htblColNameValue.get(strClusteringKey));
					// System.out.println(clusteringKey);
					if (((Comparable) htblColNameValue.get(strClusteringKey))
							.compareTo(((Comparable) getMin(i))) < 0) {
						// System.out.println(pages.size());
						break;

					}
					// System.out.println(pages.size());
					if (((Comparable) htblColNameValue.get(strClusteringKey)).compareTo(((Comparable) getMin(i))) >= 0
							&& ((Comparable) htblColNameValue.get(strClusteringKey))
									.compareTo(((Comparable) getMax(i))) <= 0) {
						// System.out.println(getMin(i));
						Page page = deserialize(pages.get(i));
						page.deleteInPageWithBS(htblColNameValue, metaOfTable, clusteringKey, primaryPos,
								strClusteringKey);
						if (page.getTuples().size() == 0) {
							File f = new File("data/" + page.getPageName() + ".class");
							System.out.println("/////||||\\\\\\\\\\\\\\\\\\deleting file " + page.getPageName());
							f.delete();
							pages.remove(i);
							//min.remove(i);
							//max.remove(i);
							i--;

						} else {
							Object minn = page.getTuples().get(0).getAttributes().get(primaryPos);
							Object maxx = page.getTuples().get(page.size() - 1).getAttributes().get(primaryPos);
							//min.setElementAt(minn, i);
							//max.setElementAt(maxx, i);
							page.serialize();
						}

					}
				}
			} else {
				Vector<Integer> attributeIndex = new Vector<>();
				Set<String> keys = htblColNameValue.keySet();
				for (String key : keys) {
					int i;
					for (i = 0; i < metaOfTable.size(); i++) {
						if (metaOfTable.get(i)[1].equals(key)) {
							break;
						}
					}
					// System.out.println(i);
					attributeIndex.add(i);
				}
				for (int i = 0; i < pages.size(); i++) {
					String pageName = pages.get(i);
					Page p = deserialize(pageName);
					// TODO: page xx is not used
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
						System.out.println("/////||||\\\\\\\\\\\\\\\\\\deleting file " + pageName);
						f.delete();
						pages.remove(i);
						//min.remove(i);
						//max.remove(i);
						i--;

					} else {
						Object minn = p.getTuples().get(0).getAttributes().get(primaryPos);
						Object maxx = p.getTuples().get(p.size() - 1).getAttributes().get(primaryPos);
						//min.setElementAt(minn, i);
						//max.setElementAt(maxx, i);
						p.serialize();
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new DBAppException("IO Exception | No such record found to delete!");
		}
	}

	public Set<Ref> getRefFromBPTree(OverflowPage OFP) throws DBAppException {
		Set<Ref> allReferences = new HashSet<Ref>();
		Vector<Ref> xx = new Vector<Ref>();
		// xx.addAll(OFP.getRefs());
		// System.out.println(xx);
		// System.out.println(OFP.getRefs().size());
		/*
		 * allReferences.add(OFP.getRefs().get(0)); for(int i = 1 ; i <
		 * OFP.getRefs().size() ; i++) {
		 * //System.out.println(OFP.getRefs().get(i).getPage().equals(OFP.getRefs().get(
		 * i-1).getPage()));
		 * if(OFP.getRefs().get(i).getPage().equals(OFP.getRefs().get(i-1).getPage()))
		 * break; else { System.out.println(OFP.getRefs().get(i));
		 * allReferences.add(OFP.getRefs().get(i)); }
		 * 
		 * }
		 */
		boolean notFound = true;
		boolean notFound1 = true;
		boolean first = true;
		for (int i = 0; i < OFP.getRefs().size(); i++) {
			if (first) {
				xx.add(OFP.getRefs().get(0));
				first = false;
			}
			for (int j = 0; j < xx.size(); j++) {
				notFound = true;
				if (OFP.getRefs().get(i).getPage().equals(xx.get(j).getPage())) {
					notFound = false;
					break;
				} else {

				}

			}
			if (notFound == true) {
				xx.add(OFP.getRefs().get(i));
			}
		}
		// allReferences.addAll(xx);
		// System.out.println(allReferences);
		// System.out.println(OFP.getRefs());
		OverflowPage nextOFP;
		boolean notNull = true;
		if (OFP.getNext() != null) {
			nextOFP = OFP.deserialize(OFP.getNext());
			while (notNull) {
				// allReferences.addAll(nextOFP.getRefs());
				for (int i = 0; i < nextOFP.getRefs().size(); i++) {
					for (int j = 0; j < xx.size(); j++) {
						notFound1 = true;
						if (nextOFP.getRefs().get(i).getPage().equals(xx.get(j).getPage())) {
							notFound1 = false;
							break;
						} else {

						}

					}
					if (notFound1 == true) {
						xx.add(nextOFP.getRefs().get(i));
					}
				}
				if (nextOFP.getNext() != null) {
					// nextOFP.serialize();
					nextOFP = nextOFP.deserialize(nextOFP.getNext());
				} else {
					notNull = false;
				}

			}
			// nextOFP.serialize();
		}
		allReferences.addAll(xx);
		// System.out.println(allReferences);
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
							Polygons p = new Polygons((Polygon) htblColNameValue.get(key));
							htblColNameValue.put(key, p);
						}
						// System.out.println(colType + " " + parameterType);
						if (!colType.equals(parameterType))
							return true;
						else
							break;
					} catch (ClassNotFoundException e) {
						throw new DBAppException("Class Not Found Exception");
					}
				}
			}
			if (i == metaOfTable.size())
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
					System.out.println("/////||||\\\\\\\\\\\\\\\\\\deleting file " + pageName);
					f.delete();
					pages.remove(i);
					//min.remove(i);
					//max.remove(i);
					i--;

				} else {
					Object minn = p.getTuples().get(0).getAttributes().get(primaryPos);
					Object maxx = p.getTuples().get(p.size() - 1).getAttributes().get(primaryPos);
					//min.setElementAt(minn, i);
					//max.setElementAt(maxx, i);
					p.serialize();
				}
			}
		}
	}

	public ArrayList<String> indicesIHave(Hashtable<String, Object> htblColNameValue,
			Hashtable<String, TreeIndex> colNameTreeIndex) {
		ArrayList<String> columns = new ArrayList<String>();

		Set<String> keys = htblColNameValue.keySet();
		for (String key : keys) {
			columns.add(key);
		}

		ArrayList<String> indices = new ArrayList<String>();

		Set<String> keys1 = colNameTreeIndex.keySet();
		for (String key1 : keys1) {
			indices.add(key1);
		}
		ArrayList<String> indicesGiven = new ArrayList<String>();
		for (int i = 0; i < indices.size(); i++) {
			for (int j = 0; j < columns.size(); j++) {
				if (indices.get(i).equals(columns.get(j))) {
					indicesGiven.add(columns.get(j));

				}
			}
		}
		/*
		 * for (String s : htblColNameValue.keySet()) {if
		 * (colNameTreeIndex.contains(s))indicesGiven.add(s); }
		 */

		return indicesGiven;
	}

	public ArrayList<String> allTableIndices(Hashtable<String, TreeIndex> colNameBTreeIndex) {
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
				if (indices.get(i).equals(clusteringKey)) {
					return true;
				}
			}
			return false;
		} else
			return false;

	}

	public void createBTreeIndex(String strColName, BPTree bTree, int colPosition) throws DBAppException, IOException {
		if (colNameTreeIndex.containsKey(strColName)) {
			throw new DBAppException("BTree index already exists on this column");
		} else {
			colNameTreeIndex.put(strColName, bTree);
		}
		for (String str : pages) {
			Page p = deserialize(str);
			Ref recordReference = new Ref(p.getPageName());

			int i = 0;
			for (Tuple t : p.getTuples()) {
				bTree.insert((Comparable) t.getAttributes().get(colPosition), recordReference);
				i++;
			}
			p.serialize();
		}
	}

	public void createRTreeIndex(String strColName, RTree rTree, int colPosition) throws DBAppException, IOException {
		if (colNameTreeIndex.containsKey(strColName)) {
			// TODO: does this "STATEMENT" saying RTREE index here work correctly?
			throw new DBAppException("RTree index already exists on this column");
		} else {
			colNameTreeIndex.put(strColName, rTree);
		}
		for (String str : pages) {
			Page p = deserialize(str);

			int i = 0;
			for (Tuple t : p.getTuples()) {
				Ref recordReference = new Ref(p.getPageName());
				rTree.insert((Comparable) t.getAttributes().get(colPosition), recordReference);
				i++;
			}
			p.serialize();
		}
	}

	public static int getIndexNumber(String pName, int s) {
		String num = pName.substring(s);
		return Integer.parseInt(num);
	}

	public static Comparable parseObject(String strTableName, Object strKey) throws DBAppException {
		try {
			Vector meta = DBApp.readFile("data/metadata.csv");
			Comparable key = null;
			for (Object O : meta) {
				String[] curr = (String[]) O;
				if (curr[0].equals(strTableName) && curr[3].equals("True")) // search in metadata for the table name and
																			// the
																			// key
				{
					if (curr[2].equals("java.lang.Integer"))
						key = (Integer) (strKey);
					else if (curr[2].equals("java.lang.Double"))
						key = (Double) (strKey);
					else if (curr[2].equals("java.util.Date"))
						key = (Date) (strKey);
					else if (curr[2].equals("java.lang.Boolean"))
						key = (Boolean) (strKey);
					else if (curr[2].equals("java.awt.Polygon"))
						key = (Polygons) strKey;
					else {
						throw new DBAppException("Searching for a key of unknown type !");
					}
				}
			}
			return key;
		} catch (ClassCastException e) {
			throw new DBAppException("Class Cast Exception");
		}
	}

	public static Comparable parseString(String strTableName, String strKey) throws DBAppException {
		try {
			Vector meta = DBApp.readFile("data/metadata.csv");
			Comparable key = null;
			for (Object O : meta) {
				String[] curr = (String[]) O;
				if (curr[0].equals(strTableName) && curr[3].equals("True")) // search in metadata for the table name and
																			// the
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
						key = (Polygons) Polygons.parsePolygons(strKey);
					else {
						throw new DBAppException("Searching for a key of unknown type !");
					}
				}
			}
			return key;
		} catch (ClassCastException e) {
			throw new DBAppException("Class Cast Exception");
		}
	}

	/**
	 * performs binary search
	 * 
	 * @param strTableName
	 * @param strKey
	 * 
	 * @return
	 * @throws DBAppException
	 */
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
				int r = p.getTuples().size() - 1;

				while (l <= r) {
					int m = l + (r - l) / 2;

					// Check if x is present at mid
					if (key.compareTo((p.getTuples().get(m)).getAttributes().get(t.getPrimaryPos())) == 0) {
						while (m > 0 && key
								.compareTo((p.getTuples().get(m - 1)).getAttributes().get(t.getPrimaryPos())) == 0) {
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
				// p.serialize(); // added by abdo
			}
			// serialize(t); // addd by abdo

			// return "-1";
			throw new DBAppException("Searched for a tuple that does not exist in the table");
		} catch (ClassCastException e) {
			throw new DBAppException("Class Cast Exception");
		}
	}

	public Iterator<Tuple> selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators,
			Vector<String[]> metaOfTable) throws DBAppException {

		// check columns names and types validity
		checkQueryValidity(arrSQLTerms, strarrOperators, metaOfTable);

		// this is for complete linear/binary search through the whole table
		// Iterator<Tuple>
		// s=inspectCols(arrSQLTerms,strarrOperators,metaOfTable);//method inspect cols
		// determines which way to search through pages
		// strarroperator operators there are checked in method setOperation and
		// getArrayOfTuples
		ArrayList<Tuple> current = new ArrayList(), next = new ArrayList();
		int i = 0, j = 0;
		for (; i < strarrOperators.length && strarrOperators[i] != null; i++)
			;
		for (; j < arrSQLTerms.length && arrSQLTerms[i] != null; j++)
			;
		if (j != i + 1)
			throw new DBAppException("Number of terms does not match number of operators");
		if (i == 0) { // only one attribute like , where id=5
			int pos = getColPositionWithinTuple(arrSQLTerms[0]._strColumnName, metaOfTable);
			current = getArrayOfTuples(arrSQLTerms[0]._strColumnName, arrSQLTerms[0]._objValue,
					arrSQLTerms[0]._strOperator, pos);
		} else {
			// boolean[]chosen=new boolean[i];
			int linearScGu = linearScanGuranteed(arrSQLTerms, strarrOperators);
			// System.out.println();
			System.out.println((linearScGu == 1) ? "linear scan"
					: (linearScGu == 2) ? "binary and indx only" : "sweet lovely indices");
			if (linearScGu == 1) {
				// at least non indexed column preceeded by or/xor , question if only the
				// cluster is the non indexed
				// preceeded by or/xor should we also do linear??
				current = doLinearScan(arrSQLTerms, strarrOperators, metaOfTable);
			} else if (linearScGu == 2) {
				// single clustering without index preceeded or sufficed with or/xor , no other
				// non indexed appears in query

				int pos = getColPositionWithinTuple(arrSQLTerms[0]._strColumnName, metaOfTable);
				current = getArrayOfTuples(arrSQLTerms[0]._strColumnName, arrSQLTerms[0]._objValue,
						arrSQLTerms[0]._strOperator, pos);
				// System.out.println(Arrays.asList(current));
				for (int k = 1; k < arrSQLTerms.length; k++) {
					pos = getColPositionWithinTuple(arrSQLTerms[k]._strColumnName, metaOfTable);
					if (strarrOperators[k - 1].toLowerCase().equals("and")) { // operation on the current
						for (int z = 0; z < current.size(); z++) {
							if (!checkTupleInCurrent(arrSQLTerms[k], current.get(z), pos)) {
								current.remove(z--);
							}
						}
					} else {
						// set operation between 2 indices
						next = getArrayOfTuples(arrSQLTerms[k]._strColumnName, arrSQLTerms[k]._objValue,
								arrSQLTerms[k]._strOperator, pos);
						current = setOperation(current, next, strarrOperators[k - 1]);
					}
				}

			} else {
				int leadingIndexPosition = getFirstIndexPos(arrSQLTerms); // index to start with
				// System.out.println(leadingIndexPosition);
				if (leadingIndexPosition == -1) {
					// no existing indices this case all the operators are ands , so will we go
					// linear
					// or if the cluster exists we search with it
					int pos = getColPositionWithinTuple(strClusteringKey, metaOfTable);
					current = (clusterExists(arrSQLTerms))
							? binaryWithCluster(arrSQLTerms, strarrOperators, metaOfTable, pos)
							: doLinearScan(arrSQLTerms, strarrOperators, metaOfTable);
				} else {// its guranteed inshallah that first index is either in first position
						// or all its previous columns are anded to it(clustering key excluded)
					String firstIndex = arrSQLTerms[leadingIndexPosition]._strColumnName;
					// System.out.println(firstIndex);
					Object value = arrSQLTerms[leadingIndexPosition]._objValue;
					String op = arrSQLTerms[leadingIndexPosition]._strOperator;
					int pos = getColPositionWithinTuple(firstIndex, metaOfTable);
					current = getArrayOfTuples(firstIndex, value, op, pos);
					// System.out.println(Arrays.asList(current));
					// filters what come before index
					for (int k = leadingIndexPosition - 1; k >= 0; k--) {
						pos = getColPositionWithinTuple(arrSQLTerms[k]._strColumnName, metaOfTable);
						// if(arrSQLTerms[k]._strColumnName.equals(strClusteringKey))
						for (int z = 0; z < current.size(); z++) {

							if (!checkTupleInCurrent(arrSQLTerms[k], current.get(z), pos)) {
								current.remove(z--);
							}
						}
					} // proceed with terms after chosen column
					for (int k = leadingIndexPosition + 1; k < arrSQLTerms.length; k++) {
						pos = getColPositionWithinTuple(arrSQLTerms[k]._strColumnName, metaOfTable);
						if (strarrOperators[k - 1].toLowerCase().equals("and")) { // operation on the current
							for (int z = 0; z < current.size(); z++) {
								if (!checkTupleInCurrent(arrSQLTerms[k], current.get(z), pos)) {
									current.remove(z--);
								}
							}
						} else {
							// set operation between 2 indices
							next = getArrayOfTuples(arrSQLTerms[k]._strColumnName, arrSQLTerms[k]._objValue,
									arrSQLTerms[k]._strOperator, pos);
							// g System.out.println(Arrays.asList(next));
							current = setOperation(current, next, strarrOperators[k - 1]);
						}
					}
				}

			}
		}

		return current.iterator();
	}

	private boolean checkTupleInCurrent(SQLTerm sqlTerm, Tuple t, int pos) throws DBAppException {
		Comparable x = (Comparable) t.getAttributes().get(pos);
		Comparable y = (Comparable) sqlTerm._objValue;
		switch (sqlTerm._strOperator) {
		case "=":
			return (x instanceof Polygons) ? x.equals(y) : x.compareTo(y) == 0;
		case "!=":
			return (x instanceof Polygons) ? !x.equals(y) : x.compareTo(y) != 0;
		case ">":
			return x.compareTo(y) > 0;
		case ">=":
			return x.compareTo(y) >= 0;
		case "<":
			return x.compareTo(y) < 0;
		case "<=":
			return x.compareTo(y) <= 0;
		default:
			throw new DBAppException("Wrong operator " + sqlTerm._strOperator);

		}
	}

	private boolean clusterExists(SQLTerm[] arrSQLTerms) {
		for (SQLTerm x : arrSQLTerms) {
			if (x._strColumnName.equals(strClusteringKey)) {
				return true;
			}
		}
		return false;
	}

	private ArrayList<Tuple> binaryWithCluster(SQLTerm[] arrSQLTerms, String[] strarrOperators,
			Vector<String[]> metaOfTable, int pos) throws DBAppException {
		ArrayList<Tuple> res = new ArrayList();

		for (int i = 0; i < arrSQLTerms.length; i++) {
			if (arrSQLTerms[i]._strColumnName.equals(strClusteringKey)) {
				res = getArrayOfTuples(strClusteringKey, arrSQLTerms[i]._objValue, arrSQLTerms[i]._strOperator,
						primaryPos);
				break;

			}
		}
		for (int i = 0; i < arrSQLTerms.length; i++) {
			if (arrSQLTerms[i]._strColumnName.equals(strClusteringKey))
				continue;
			for (int z = 0; z < res.size(); z++) {
				if (!checkTupleInCurrent(arrSQLTerms[i], res.get(z),
						getColPositionWithinTuple(arrSQLTerms[i]._strColumnName, metaOfTable))) {
					res.remove(z--);
				}
			}
		}
		return res;
	}

	private int linearScanGuranteed(SQLTerm[] arrSQLTerms, String[] strarrOperators) {
		// for checking , cluster??
		boolean found = false;
		boolean clustNondIdx = false;
		boolean nonClustNonIdx = false;

		boolean abod_eso = true;

		if (arrSQLTerms[0]._strOperator.equals("!="))
			return 1;

		if (!colNameTreeIndex.containsKey(arrSQLTerms[0]._strColumnName)) {// first is non indexed
			if (!arrSQLTerms[0]._strColumnName.equals(strClusteringKey)) {// non cluster
				if (!strarrOperators[0].toLowerCase().equals("and")) // sufficed with or/xor then must be linear
					return 1;
				else
					nonClustNonIdx = true; // at least one non indexed exists
			} else if (arrSQLTerms[0]._strColumnName.equals(strClusteringKey)
					&& !strarrOperators[0].toLowerCase().equals("and"))
				clustNondIdx = true;// clustering non indexed sufficed with or/xor
		} else {
			found = true;
		}

		for (int i = 1; i < arrSQLTerms.length; i++) {
			if (arrSQLTerms[i]._strOperator.equals("!="))
				return 1;
			if (colNameTreeIndex.containsKey(arrSQLTerms[i]._strColumnName)) { // if first index preceeded by or/xor it
																				// should be linear;
				if (strarrOperators[i - 1].toLowerCase().equals("and"))
					found = true;
				else if (arrSQLTerms[i - 1]._strColumnName.equals(strClusteringKey)) // cluster=c or index=d
					found = true;
				else if (!found)
					abod_eso = false;// return 1;
			}
			if (arrSQLTerms[i]._strColumnName.equals(strClusteringKey)
					&& !colNameTreeIndex.containsKey(strClusteringKey)
					&& !strarrOperators[i - 1].toLowerCase().equals("and")) {
				clustNondIdx = true;
				if (nonClustNonIdx)
					return 1;
				continue;
			}

			if (!colNameTreeIndex.containsKey(arrSQLTerms[i]._strColumnName)) {
				if (!strarrOperators[i - 1].toLowerCase().equals("and"))
					return 1;
				else if (clustNondIdx)
					return 1;
				nonClustNonIdx = true;
			}

		}
		if (clustNondIdx && !nonClustNonIdx)
			return 2; // only single clustering non indexed sufficed or preceeded with or/xor
		if (!abod_eso)
			return 1;
		return 0;
	}

	private int getFirstIndexPos(SQLTerm[] arrSQLTerms) {
		for (int i = 0; i < arrSQLTerms.length; i++) {
			if (colNameTreeIndex.containsKey(arrSQLTerms[i]._strColumnName))
				return i;
		}
		return -1;
	}

	private ArrayList<Tuple> doLinearScan(SQLTerm[] arrSQLTerms, String[] strarrOperators, Vector<String[]> metaOfTable)
			throws DBAppException {
		// get Array of positions of columns relative to tuple
		ArrayList<Integer> x = new ArrayList();
		ArrayList<Tuple> res = new ArrayList();
		for (SQLTerm st : arrSQLTerms) {
			int i = 0;
			for (; i < metaOfTable.size(); i++) {
				if (st._strColumnName.equals(metaOfTable.get(i)[1]))
					break;
			}
			x.add(i);
		}
		for (int i = 0; i < pages.size(); i++) {
			Page p = deserialize(pages.get(i));
			for (Tuple t : p.getTuples()) {
				if (tupleMetConditions(arrSQLTerms, strarrOperators, x, x.size() - 1, t))
					res.add(t);
			}
		}
		return res;
	}

	private boolean tupleMetConditions(SQLTerm[] arrSQLTerms, String[] strarrOperators, ArrayList<Integer> x, int i,
			Tuple t) throws DBAppException {
		switch (strarrOperators[i - 1].toLowerCase()) {
		case "or":
			return (i == 1)
					? checkTupleInCurrent(arrSQLTerms[0], t, x.get(0))
							|| checkTupleInCurrent(arrSQLTerms[1], t, x.get(1))
					: tupleMetConditions(arrSQLTerms, strarrOperators, x, i - 1, t)
							|| checkTupleInCurrent(arrSQLTerms[i], t, x.get(i));
		case "and":
			return (i == 1)
					? checkTupleInCurrent(arrSQLTerms[0], t, x.get(0))
							&& checkTupleInCurrent(arrSQLTerms[1], t, x.get(1))
					: tupleMetConditions(arrSQLTerms, strarrOperators, x, i - 1, t)
							&& checkTupleInCurrent(arrSQLTerms[i], t, x.get(i));
		case "xor":
			return (i == 1)
					? checkTupleInCurrent(arrSQLTerms[0], t, x.get(0))
							^ checkTupleInCurrent(arrSQLTerms[1], t, x.get(1))
					: tupleMetConditions(arrSQLTerms, strarrOperators, x, i - 1, t)
							^ checkTupleInCurrent(arrSQLTerms[i], t, x.get(i));
		default:
			return false;
		}
	}

	private ArrayList<Tuple> setOperation(ArrayList<Tuple> current, ArrayList<Tuple> next, String string) {
		string = string.toLowerCase();
		ArrayList<Tuple> res = new ArrayList();
		switch (string) {
		case "or":
			res = orSets(current, next);
			break;
		// case "and":res=andSets(current,next);break;
		case "xor":
			res = xorSets(current, next);
			break;
		// default:throw new DBAppException("wrong operation type "+string);
		}
		return res;
	}

	private ArrayList<Tuple> xorSets(ArrayList<Tuple> current, ArrayList<Tuple> next) {
		return differenceSets(orSets(current, next), andSets(current, next));
	}

	// private ArrayList<Tuple> differenceSets(ArrayList<Tuple> current,
	// ArrayList<Tuple> next) {
	// ArrayList<Tuple> res=new ArrayList();
	// for(Tuple t:current) {
	// if(!next.contains(t))
	// res.add(t);
	// }
	// return res;
	// }
	public ArrayList<Tuple> differenceSets(ArrayList<Tuple> A, ArrayList<Tuple> B) {
		ArrayList<Tuple> res = new ArrayList<>();

		HashSet<Tuple> first = new HashSet<>();
		HashSet<Tuple> second = new HashSet<>();

		for (int i = 0; i < B.size(); i++) {
			Tuple cur = B.get(i);
			second.add(cur);
		}
		for (int i = 0; i < A.size(); i++) {
			Tuple cur = A.get(i);
			if (first.contains(cur))
				continue;
			first.add(cur);
			if (!second.contains(cur)) {
				res.add(cur);
			}
		}
		return res;
	}

	// private ArrayList<Tuple> andSets(ArrayList<Tuple> current, ArrayList<Tuple>
	// next) {
	// ArrayList<Tuple> res=new ArrayList();
	// for(Tuple t:current) {
	// if(next.contains(t))
	// res.add(t);
	// }
	// return res;
	// }
	public ArrayList<Tuple> andSets(ArrayList<Tuple> current, ArrayList<Tuple> next) {
		ArrayList<Tuple> res = new ArrayList<>();

		HashSet<Tuple> first = new HashSet<>();
		HashSet<Tuple> second = new HashSet<>();
		for (int i = 0; i < current.size(); i++) {
			Tuple cur = current.get(i);
			first.add(cur);
		}
		for (int i = 0; i < next.size(); i++) {
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
		Set<Tuple> x = new HashSet();
		x.addAll(current);
		x.addAll(next);
		ArrayList<Tuple> res = new ArrayList();
		res.addAll(x);
		return res;
	}

	private ArrayList<Tuple> getArrayOfTuples(String _strColumnName, Object _objValue, String _strOperator, int pos)
			throws DBAppException {
		if (_strOperator.equals("!="))
			return goLinear(_strColumnName, _objValue, _strOperator, pos);
		// System.out.println(_strOperator);

		return (colNameTreeIndex.containsKey(_strColumnName))
				? goWithIndex(_strColumnName, _objValue, _strOperator, pos)
				: (_strColumnName.equals(strClusteringKey)) ? goBinary(_strColumnName, _objValue, _strOperator, pos)
						: goLinear(_strColumnName, _objValue, _strOperator, pos);
	}

	private int getColPositionWithinTuple(String _strColumnName, Vector<String[]> metaOfTable) {
		for (int i = 0; i < metaOfTable.size(); i++) {
			if (metaOfTable.get(i)[1].equals(_strColumnName))
				return i;
		}
		return -1;
	}

	private boolean validOp(String _strOperator) {
		return _strOperator.equals("=") || _strOperator.equals("!=") || _strOperator.equals(">")
				|| _strOperator.equals(">=") || _strOperator.equals("<") || _strOperator.equals("<=");
	}

	private ArrayList<Tuple> goLinear(String _strColumnName, Object _objValue, String _strOperator, int pos)
			throws DBAppException {
		// TODO
		ArrayList<Tuple> res = new ArrayList();
		switch (_strOperator) {
		case ">":
		case ">=":
			res = mtOrMtlLinear(_strColumnName, _objValue, _strOperator, pos);
			break;
		case "<":
		case "<=":
			res = ltOrLtlLinear(_strColumnName, _objValue, _strOperator, pos);
			break;
		case "=":
			res = equalsLinear(_strColumnName, _objValue, _strOperator, pos);
			break;
		case "!=":
			res = notEqualsLinear(_strColumnName, _objValue, _strOperator, pos);
			break;
		}
		return res;
	}

	private ArrayList<Tuple> notEqualsLinear(String _strColumnName, Object _objValue, String _strOperator, int pos)
			throws DBAppException {
		ArrayList<Tuple> res = new ArrayList();
		for (int i = 0; i < pages.size(); i++) {
			Page x = deserialize(pages.get(i));
			for (int j = 0; j < x.getTuples().size(); j++) {
				// TODO POLYGON RECHECK

				Comparable esoKey = (Comparable) x.getTuples().get(j).getAttributes().get(pos);
				Comparable objValue = (Comparable) _objValue;
				if (esoKey.compareTo(objValue) != 0) {
					res.add(x.getTuples().get(j));
				} else if ((esoKey instanceof Polygons) && !esoKey.equals(objValue)) {
					res.add(x.getTuples().get(j));
				}

			}

		}
		return res;
	}

	private ArrayList<Tuple> equalsLinear(String _strColumnName, Object _objValue, String _strOperator, int pos)
			throws DBAppException {
		ArrayList<Tuple> res = new ArrayList();
		for (int i = 0; i < pages.size(); i++) {
			Page x = deserialize(pages.get(i));
			for (int j = 0; j < x.getTuples().size(); j++) {
				// TODO POLYGON RECHECK
				Comparable esoKey = (Comparable) x.getTuples().get(j).getAttributes().get(pos);
				Comparable objValue = (Comparable) _objValue;
				if (esoKey.compareTo(objValue) == 0) {
					if (!(esoKey instanceof Polygons) || esoKey.equals(objValue))
						res.add(x.getTuples().get(j));
				}
			}
		}
		return res;
	}

	private ArrayList<Tuple> ltOrLtlLinear(String _strColumnName, Object _objValue, String _strOperator, int pos)
			throws DBAppException {

		ArrayList<Tuple> res = new ArrayList();
		for (int i = 0; i < pages.size(); i++) {
			if (((Comparable) getMin(i)).compareTo((Comparable) _objValue) > 0)
				break;
			if (((Comparable) getMin(i)).compareTo((Comparable) _objValue) == 0 && _strOperator.length() == 1)
				break;
			// TOTO:Polygons; line before; NO NEED as <=/>= is based on area; no points
			// ==>CompareTo is fine here
			Page x = deserialize(pages.get(i));
			int j = 0;
			while (j < x.getTuples().size() && ((Comparable) x.getTuples().get(j).getAttributes().get(pos))
					.compareTo((Comparable) _objValue) < 0)
				res.add(x.getTuples().get(j++));
			if (_strOperator.length() == 2) {
				while (j < x.getTuples().size() && ((Comparable) x.getTuples().get(j).getAttributes().get(pos))
						.compareTo((Comparable) _objValue) == 0)
					// TODO:Polygons; line before ; NO NEED as <=/>= is based on area; no points
					// ==>CompareTo is fine here
					res.add(x.getTuples().get(j++));
			}

		}
		return res;

	}

	private ArrayList<Tuple> mtOrMtlLinear(String _strColumnName, Object _objValue, String _strOperator, int pos)
			throws DBAppException {
		ArrayList<Tuple> res = new ArrayList();
		// System.out.println("p"+pages.size());
		// System.out.println("m"+max.size());
		for (int i = pages.size() - 1; i >= 0; i--) {
			System.out.println(i);
			if (((Comparable) getMax(i)).compareTo((Comparable) _objValue) < 0)
				break;
			if (((Comparable) getMax(i)).compareTo((Comparable) _objValue) == 0 && _strOperator.length() == 1)
				break;
			Page x = deserialize(pages.get(i));
			int j = x.getTuples().size() - 1;
			while (j >= 0 && ((Comparable) x.getTuples().get(j).getAttributes().get(pos))
					.compareTo((Comparable) _objValue) > 0) {
				res.add(0, x.getTuples().get(j));
				j--;
			}
			if (_strOperator.length() == 2) {
				while (j >= 0 && ((Comparable) x.getTuples().get(j).getAttributes().get(pos))
						.compareTo((Comparable) _objValue) == 0) {
					res.add(0, x.getTuples().get(j));
					j--;
				}
			}
		}
		return res;
	}

	private ArrayList<Tuple> goBinary(String _strColumnName, Object _objValue, String _strOperator, int pos)
			throws DBAppException {
		// TODO Auto-generated method stub
		ArrayList<Tuple> res = new ArrayList();
		// System.out.println(_strOperator);
		switch (_strOperator) {
		case ">":
		case ">=":
			res = mtOrMtlBinary(_strColumnName, _objValue, _strOperator, pos);
			break;
		case "<":
		case "<=":
			res = ltOrLtlBinary(_strColumnName, _objValue, _strOperator, pos);
			break;
		case "=":
			res = equalsBinary(_strColumnName, _objValue, _strOperator, pos);
			break;
		}
		return res;

	}

	private ArrayList<Tuple> equalsBinary(String _strColumnName, Object _objValue, String _strOperator, int pos)
			throws DBAppException {
		String[] searchResult = SearchInTable(tableName, _objValue).split("#");
		// System.out.println(searchResult.length);
		String startPage = searchResult[0];
		int startPageIndex = getPageIndex(startPage);
		int startTupleIndex = Integer.parseInt(searchResult[1]);
		// System.out.println(startPageIndex+","+startTupleIndex);
		ArrayList<Tuple> res = new ArrayList<>();
		for (int pageIdx = startPageIndex, tupleIdx = startTupleIndex; pageIdx < pages
				.size(); pageIdx++, tupleIdx = 0) {
			// TODO: We are looping LINEARLY OVER PAGES ? Should we make it binary ??
			if (((Comparable) getMin(pageIdx)).compareTo((Comparable) _objValue) > 0)
				break;
			Page currentPage = deserialize(pages.get(pageIdx));
			Comparable cmp;
			while (tupleIdx < currentPage.getTuples().size()
					&& (cmp = (Comparable) currentPage.getTuples().get(tupleIdx).getAttributes().get(pos))
							.compareTo(_objValue) == 0)
				// TODO:Polygons; line before:: Ya rab tsht8l
				if (!(cmp instanceof Polygons) || cmp.equals(_objValue))
					res.add(currentPage.getTuples().get(tupleIdx++));
		}

		return res;
	}

	private ArrayList<Tuple> ltOrLtlBinary(String _strColumnName, Object _objValue, String _strOperator, int pos)
			throws DBAppException {
		return ltOrLtlLinear(_strColumnName, _objValue, _strOperator, pos);
	}

	private ArrayList<Tuple> mtOrMtlBinary(String _strColumnName, Object _objValue, String _strOperator, int pos)
			throws DBAppException {
		return mtOrMtlLinear(_strColumnName, _objValue, _strOperator, pos);

	}

	private ArrayList<Tuple> goWithIndex(String _strColumnName, Object _objValue, String _strOperator, int pos)
			throws DBAppException {
		ArrayList<Tuple> res = new ArrayList();
		switch (_strOperator) {
		case ">":
		case ">=":
			res = mtOrMtlIndex(_strColumnName, _objValue, _strOperator, pos);
			break;
		case "<":
		case "<=":
			res = ltOrLtlIndex(_strColumnName, _objValue, _strOperator, pos);
			break;
		case "=":
			res = equalsIndex(_strColumnName, _objValue, _strOperator, pos);
			break;
		// case "=":res=equalsLinear(_strColumnName, _objValue, _strOperator,pos);break;
		}
		return res;
	}

	private ArrayList<Tuple> equalsIndex(String _strColumnName, Object _objValue, String _strOperator, int pos)
			throws DBAppException {
		ArrayList<Tuple> res = new ArrayList<>();
		String lastPage = pages.get(pages.size() - 1);
		int lastPageMaxNum = Integer.parseInt(lastPage.substring(tableName.length()));
		boolean[] visited = new boolean[lastPageMaxNum + 1];
		TreeIndex b = colNameTreeIndex.get(_strColumnName);
		GeneralReference resultReference = b.search((Comparable) _objValue);
		if (resultReference == null)
			return res;
		ArrayList<Ref> referenceList = resultReference.getALLRef();
		System.out.println(Arrays.asList(referenceList));
		for (int i = 0; i < referenceList.size(); i++) {
			Ref currentReference = referenceList.get(i);
			String pagename = currentReference.getPage();
			int curPageNum = Integer.parseInt(pagename.substring(tableName.length()));
			System.out.println(curPageNum);
			if (visited[curPageNum])
				continue;
			addToResultSet(res, pagename, pos, _objValue, _strOperator);
			visited[curPageNum] = true;
		}
		return res;
	}

	private ArrayList<Tuple> ltOrLtlIndex(String _strColumnName, Object _objValue, String _strOperator, int pos)
			throws DBAppException {
		if (_strColumnName.equals(strClusteringKey))
			return ltOrLtlLinear(_strColumnName, _objValue, _strOperator, pos);
		ArrayList<Tuple> res = new ArrayList();
		String lastPage = pages.get(pages.size() - 1);
		int lastPageMaxNum = Integer.parseInt(lastPage.substring(tableName.length()));
		boolean[] visited = new boolean[lastPageMaxNum + 1];
		TreeIndex b = colNameTreeIndex.get(_strColumnName);
		LeafNode leaf = b.getLeftmostLeaf();
		while (leaf != null) {
			int i;
			for (i = 0; i < leaf.getNumberOfKeys(); i++) {
				GeneralReference gr = leaf.getRecord(i);
				if (leaf.getKey(i).compareTo((Comparable) _objValue) > 0)
					break;
				if (leaf.getKey(i).compareTo((Comparable) _objValue) == 0 && _strOperator.length() == 1)
					break;
				Set<Ref> ref = fillInRef(gr);
				// System.out.println(Arrays.asList(ref));
				for (Ref r : ref) {
					String pagename = r.getPage();
					int curPageNum = Integer.parseInt(pagename.substring(tableName.length()));
					if (visited[curPageNum])
						continue;
					System.out.println("DDDDDDDDDDDEBUG" + curPageNum);
					addToResultSet(res, pagename, pos, _objValue, _strOperator);
					visited[curPageNum] = true;
				}

			}
			if (i < leaf.getNumberOfKeys())
				break;
			leaf = leaf.getNext();
		}
		return res;
	}

	private void addToResultSet(ArrayList<Tuple> res, String pagename, int pos, Object _objValue, String _strOperator)
			throws DBAppException {
		switch (_strOperator) {
		case ("<"):
			addToResultSetLESS(res, pagename, pos, _objValue);
			break;
		case ("<="):
			addToResultSetLESSorEQUAL(res, pagename, pos, _objValue);
			break;
		case ("="):
			addToResultSetEQUAL(res, pagename, pos, _objValue);
			break;
		case (">"):
			addToResultSetMORE(res, pagename, pos, _objValue);
			break;
		case (">="):
			addToResultSetMOREorEQUAL(res, pagename, pos, _objValue);
			break;
		default:
			throw new DBAppException("55555555555555");
		}
	}

	private void addToResultSetLESS(ArrayList<Tuple> res, String pagename, int pos, Object _objValue)
			throws DBAppException {
		Page x = deserialize(pagename);
		System.out.println(">>>><<<<< " + pagename);
		for (int i = 0; i < x.getTuples().size(); i++) {
			if (((Comparable) _objValue).compareTo((Comparable) x.getTuples().get(i).getAttributes().get(pos)) > 0)
				res.add(x.getTuples().get(i));
			// else break;
		}
		return;
	}

	private void addToResultSetLESSorEQUAL(ArrayList<Tuple> res, String pagename, int pos, Object _objValue)
			throws DBAppException {
		Page x = deserialize(pagename);
		for (int i = 0; i < x.getTuples().size(); i++) {
			if (((Comparable) _objValue).compareTo((Comparable) x.getTuples().get(i).getAttributes().get(pos)) >= 0)
				res.add(x.getTuples().get(i));
			// else break;
		}
		return;
	}

	private void addToResultSetEQUAL(ArrayList<Tuple> res, String pagename, int pos, Object _objValue)
			throws DBAppException {
		Page x = deserialize(pagename);
		for (int i = 0; i < x.getTuples().size(); i++) {
			if (((Comparable) _objValue).compareTo((Comparable) x.getTuples().get(i).getAttributes().get(pos)) == 0)
				res.add(x.getTuples().get(i));
			// else
			// if(((Comparable)_objValue).compareTo((Comparable)x.getTuples().get(i).getAttributes().get(pos))<0)
			// {
			// //TODO:make sure it is that I finished the records (EQUAL) and now in the
			// records > my key; not the opposite
			// // break;
			// }
		}
		return;
	}

	private void addToResultSetMORE(ArrayList<Tuple> res, String pagename, int pos, Object _objValue)
			throws DBAppException {
		Page x = deserialize(pagename);
		for (int i = 0; i < x.getTuples().size(); i++) {
			if (((Comparable) _objValue).compareTo((Comparable) x.getTuples().get(i).getAttributes().get(pos)) < 0)
				res.add(x.getTuples().get(i));
		}
		return;
	}

	private void addToResultSetMOREorEQUAL(ArrayList<Tuple> res, String pagename, int pos, Object _objValue)
			throws DBAppException {
		Page x = deserialize(pagename);
		for (int i = 0; i < x.getTuples().size(); i++) {
			if (((Comparable) _objValue).compareTo((Comparable) x.getTuples().get(i).getAttributes().get(pos)) <= 0)
				res.add(x.getTuples().get(i));
		}
		return;
	}

	private Set<Ref> fillInRef(GeneralReference gr) throws DBAppException {
		Set<Ref> ref = new HashSet();
		if (gr instanceof Ref)
			ref.add((Ref) gr);
		else {
			OverflowReference ov = (OverflowReference) gr;
			OverflowPage ovp = ov.getFirstPage();
			while (ovp != null) {
				for (Ref r : ovp.getRefs())
					ref.add(r);
				ovp = ovp.getNext1();

			}
		}
		return ref;
	}

	private ArrayList<Tuple> mtOrMtlIndex(String _strColumnName, Object _objValue, String _strOperator, int pos)
			throws DBAppException {
		if (_strColumnName.equals(strClusteringKey))
			return mtOrMtlLinear(_strColumnName, _objValue, _strOperator, pos);
		else {
			ArrayList<Tuple> res = new ArrayList<>();
			String lastPage = pages.get(pages.size() - 1);
			int lastPageMaxNum = Integer.parseInt(lastPage.substring(tableName.length()));
			boolean[] visited = new boolean[lastPageMaxNum + 1];
			TreeIndex b = colNameTreeIndex.get(_strColumnName);
			ArrayList<GeneralReference> referenceList = _strOperator.equals(">") ? b.searchMT((Comparable) _objValue)
					: b.searchMTE((Comparable) _objValue);
			// searchMT_MTE((Comparable)_objValue);
			// ArrayList<Ref> referenceList = resultReference.getALLRef();
			for (int i = 0; i < referenceList.size(); i++) {
				GeneralReference currentGR = referenceList.get(i);
				ArrayList<Ref> currentRefsForOneKey = currentGR.getALLRef();
				for (int j = 0; j < currentRefsForOneKey.size(); j++) {
					Ref currentReference = currentRefsForOneKey.get(j);
					String pagename = currentReference.getPage();
					int curPageNum = Integer.parseInt(pagename.substring(tableName.length()));
					if (visited[curPageNum])
						continue;
					addToResultSet(res, pagename, pos, _objValue, _strOperator);
					visited[curPageNum] = true;
				}
			}
			return res;
		}
	}

	// private ArrayList<Tuple> ALTERNATIVEltOrLtlIndex(String _strColumnName,
	// Object _objValue, String _strOperator, int pos) throws DBAppException {
	// if(_strColumnName.equals(strClusteringKey))
	// return mtOrMtlLinear(_strColumnName, _objValue, _strOperator, pos);
	// else {
	// ArrayList<Tuple> res = new ArrayList<>();
	// String lastPage=pages.get(pages.size()-1);
	// int lastPageMaxNum=Integer.parseInt(lastPage.substring(tableName.length()));
	// boolean []visited=new boolean[lastPageMaxNum];
	// TreeIndex b=colNameTreeIndex.get(_strColumnName);
	// ArrayList<GeneralReference> referenceList = _strOperator.equals("<")?
	// b.searchlT((Comparable)_objValue) :
	// b.searchlTE((Comparable)_objValue) ;
	//// searchMT_MTE((Comparable)_objValue);
	//// ArrayList<Ref> referenceList = resultReference.getALLRef();
	// for (int i=0;i<referenceList.size();i++) {
	// GeneralReference currentGR= referenceList.get(i);
	// ArrayList<Ref> currentRefsForOneKey= currentGR.getALLRef();
	// for (int j=0;j<currentRefsForOneKey.size();j++) {
	// Ref currentReference = currentRefsForOneKey.get(j);
	// String pagename = currentReference.getPage();
	// int curPageNum=Integer.parseInt(pagename.substring(tableName.length()));
	// if (visited[curPageNum]) continue;
	// addToResultSet(res, pagename, pos, _objValue, _strOperator);
	// visited[curPageNum] = true;
	// }
	// }
	// return res;
	// }
	// }
	//

	public void checkQueryValidity(SQLTerm[] arrSQLTerms, String[] strarrOperators, Vector<String[]> metaOfTable)
			throws DBAppException {
		// boolean clusterHasIndex=false;
		for (SQLTerm x : arrSQLTerms) {
			int i;
			if (!validOp(x._strOperator))
				throw new DBAppException("Wrong or unsupported operator " + x._strOperator);
			for (i = 0; i < metaOfTable.size(); i++) {
				if (!x._strTableName.equals(arrSQLTerms[0]._strTableName))
					throw new DBAppException("Different table name " + x._strTableName + ", we do not support joins");
				if (metaOfTable.get(i)[1].equals(x._strColumnName)) {
					try {
						Class colType = Class.forName(metaOfTable.get(i)[2]);

						Class parameterType = x._objValue.getClass();
						Class polyOriginal = Class.forName("java.awt.Polygon");
						if (colType == polyOriginal) {
							// TODO:Don't forget to test this !!!
							x._objValue = new Polygons((Polygon) x._objValue);
						}
						if (!colType.equals(parameterType)) {
							throw new DBAppException("DATA types 8alat");
						} else {
							break;
						}

					} catch (ClassNotFoundException e) {
						throw new DBAppException("Class Not Found Exception");
					}
					// if(metaOfTable.get(i)[3].equals("True")&&metaOfTable.get(i)[3].equals("True"))
					// clusterHasIndex=true;
				}
			}
			if (i == metaOfTable.size())
				throw new DBAppException("Column " + x._strColumnName + " doesn't exist");
		}
		for (String x : strarrOperators) {
			x = x.toLowerCase();
			if (!(x.equals("and") || x.equals("xor") || x.equals("or")))
				throw new DBAppException("Wrong or unsupported bitwise operation " + x);
		}
	}

	public void drop() throws DBAppException {
		for (String k : pages) {
			File fileIn = new File("data/" + k + ".class");
			System.out.println("/////||||\\\\\\\\\\\\\\\\\\deleting file " + k);
			fileIn.delete();
		}
		// TODO: delete indices
	}

	public int getPageIndex(String pageName) {
		int pageOriginalNum = getSuffix(pageName);
		int i = pageOriginalNum;
		for (; i >= 0 && getSuffix(pages.get(i)) > pageOriginalNum; i--)
			;
		return i;
	}

	public int getSuffix(String pageName) {
		return Integer.parseInt(pageName.substring(tableName.length()));
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();

		sb.append("Name: " + tableName + "\n");

		sb.append("Clustering key: " + strClusteringKey + " @ pos=" + primaryPos + "\n");

		sb.append("Pages:\n{");
		for (int i = 0; i < pages.size() - 1; i++) {
			sb.append(pages.get(i) + ", ");
		}
		if (pages.size() > 0)
			sb.append(pages.get(pages.size() - 1) + "}\n");

		/*
		 * sb.append("Min:\n{"); for (int i = 0; i < pages.size() - 1; i++) {
		 * sb.append(getMin(i) + ", "); } if (pages.size() > 0)
		 * sb.append(getMin(pages.size() - 1) + "}\n");
		 * 
		 * sb.append("Max:\n{"); for (int i = 0; i < pages.size() - 1; i++) {
		 * sb.append(getMax(i) + ", "); } if (pages.size() > 0)
		 * sb.append(getMax(pages.size() - 1) + "}\n");
		 */
		sb.append("Indexed Columns: \n");
		for (String col : colNameTreeIndex.keySet()) {
			sb.append(col + "\t");
		}
		sb.append("Indexes: \n");
		for (String col : colNameTreeIndex.keySet()) {
			sb.append(col + "\n");
			sb.append(colNameTreeIndex.get(col) + "\n");
		}
		return sb.toString();
	}

	static void tstGettingPageIndexFromName(String[] args) {
		Table t = new Table();
		t.setTableName("Tab");
		Vector<String> pg = t.pages;
		for (int i = 0; i < 20; i++) {
			pg.add("Tab" + i);
		}
		for (int i = 0; i < 20; i++) {
			System.out.printf("indx of Tab%d=%d\n", i, t.getPageIndex("Tab" + i));
		}
		int i = 0;
		pg.remove(10);
		pg.remove(10);
		pg.remove(5);
		for (i = 0; i < 18; i++) {
			System.out.printf("indx of Tab%d=%d\n", i, t.getPageIndex("Tab" + i));
			System.out.println(pg.get(t.getPageIndex("Tab" + i)));
		}
	}

	static String show(ArrayList<Tuple> arr) {
		StringBuilder sb = new StringBuilder();
		sb.append("{");
		for (int i = 0; i < arr.size() - 1; i++) {
			sb.append(arr.get(i).getAttributes().get(0) + ", ");
		}
		if (arr.size() > 0)
			sb.append(arr.get(arr.size() - 1).getAttributes().get(0));
		sb.append("}");
		return sb.toString();
	}

	public static void main(String[] args) {
		ArrayList<Tuple> arr1 = new ArrayList<>();
		ArrayList<Tuple> arr2 = new ArrayList<>();
		int n = (int) (1 + Math.random() * 5);
		int m = (int) (1 + Math.random() * 5);
		for (int i = 0; i < n; i++) {
			Tuple t = new Tuple();
			t.addAttribute((int) (Math.random() * 9));
			arr1.add(t);
		}
		for (int i = 0; i < m; i++) {
			Tuple t = new Tuple();
			t.addAttribute((int) (Math.random() * 9));
			arr2.add(t);
		}
		System.out.println(show(arr1));
		System.out.println(show(arr2));
		Table k = new Table();
		ArrayList<Tuple> and = k.andSets(arr1, arr2);
		ArrayList<Tuple> or = k.orSets(arr1, arr2);
		ArrayList<Tuple> xor = k.xorSets(arr1, arr2);
		System.out.println("And=" + show(and));
		System.out.println("Or=" + show(or));
		System.out.println("Xor=" + show(xor));

	}

}
