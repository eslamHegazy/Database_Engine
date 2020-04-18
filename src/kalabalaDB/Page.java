package kalabalaDB;

import java.io.*;
import java.util.*;

import BPTree.BPTree;
import General.GeneralReference;
import General.OverflowPage;
import General.OverflowReference;
import General.Ref;
import General.TreeIndex;

public class Page implements Serializable {
	private Vector<Tuple> tuples;
	private String pageName;
	private static int lastIn = 0;

	public Page(String pageName) {
		tuples = new Vector<Tuple>();
		this.pageName = pageName;
	}

	public Vector<Tuple> getTuples() {
		return tuples;
	}

	public void setTuples(Vector<Tuple> tuples) {
		this.tuples = tuples;
	}

	public String getPageName() {
		return pageName;
	}

	public void setPageName(String pageName) {
		this.pageName = pageName;
	}

	public int getLastIn() {
		return lastIn;
	}

	public void setLastIn(int lastIn) {
		this.lastIn = lastIn;
	}

	public int size() {
		return tuples.size();
	}

	public int binarySearch(Comparable key, int pos) {
		int ans = bsLastOcc(key, pos);
		if (ans == -1) {
			ans = bsFirstGreater(key, pos);
			if (ans == -1) {
				ans = tuples.size();
			}
		}
		return ans;

	}

	public int bsLastOcc(Comparable key, int pos) {
		int ans = -1;
		int lo = 0;
		int hi = tuples.size() - 1;
		int med = lo + (hi - lo + 1) / 2;
		while (lo <= hi) {
			med = lo + (hi - lo + 1) / 2;
			Comparable curVal = (Comparable) tuples.get(med).getAttributes().get(pos);
			if (curVal.compareTo(key) < 0) {
				lo = med + 1;
			} else if (curVal.compareTo(key) == 0) {
				lo = med + 1;
				ans = med;
			} else {// curVal>med
				hi = med - 1;
			}
		}
		return ans;
	}

	public int bsFirstGreater(Comparable key, int pos) {
		int ans = -1;
		int lo = 0;
		int hi = tuples.size() - 1;
		int med = lo + (hi - lo + 1) / 2;
		while (lo <= hi) {
			med = lo + (hi - lo + 1) / 2;
			Comparable curVal = (Comparable) tuples.get(med).getAttributes().get(pos);
			if (curVal.compareTo(key) <= 0) {
				lo = med + 1;
			} else {// curVal>med
				ans = med;
				hi = med - 1;
			}
		}
		return ans;
	}

	public void insertIntoPage(Tuple x, int pos) {
		Comparable nKey = (Comparable) x.getAttributes().get(pos);
//		int lower = 0;
//		int upper = tuples.size() - 1;
//		if (upper == -1) {
//			tuples.addElement(x);
//		} else {
//			int ans = binarySearch(nKey, pos);
//			if (ans == tuples.size()) {
//				tuples.add(x);
//			} else {
//				tuples.insertElementAt(x, ans);
//			}
//		}
		for(int i=0;i<tuples.size();i++){
			if(nKey.compareTo(tuples.get(i).getAttributes().get(pos))<0){
				tuples.insertElementAt(x, i);
				return;
			}
		}
		tuples.insertElementAt(x, tuples.size());
	}

	public void serialize() throws DBAppException {
		try {
			System.out.println("IO||||\t serialize:page:"+pageName);
			FileOutputStream fileOut = new FileOutputStream("data/" + pageName + ".class");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(this);
			out.close();
			fileOut.close();
		} catch (IOException e) {
			throw new DBAppException("IO Exception");
		}
	}

	public void deleteInPage(Hashtable<String, Object> htblColNameValue, Vector<Integer> attributeIndex) {

		for (int i = 0; i < tuples.size(); i++) {
			Vector x = tuples.get(i).getAttributes();
			Set<String> keys = htblColNameValue.keySet();
			int j = 0;
			for (String key : keys) {
				if (j == attributeIndex.size()) {
					break;
				}
				if (!x.get(attributeIndex.get(j)).equals(htblColNameValue.get(key))) {
					break;
				}
				j++;
			}
			if (j == attributeIndex.size()) {
				tuples.remove(i);
				i--;
			}
		}
	}

	public void deleteInPageforRef(Vector<String[]> metaOfTable, int primarypos, String clusteringKey,
			Hashtable<String, TreeIndex> colNameTreeIndex, Hashtable<String, Object> htblColNameValue,
			ArrayList<String> allIndices, boolean isCluster) throws DBAppException,IOException {
		int index = 0;
		int lastOcc = tuples.size();
		if (isCluster) {
			lastOcc = bsLastOcc((Comparable) htblColNameValue.get(clusteringKey), primarypos) + 1;
			for (index = lastOcc - 1; index >= 0 && ((Comparable)tuples.get(index).getAttributes().get(primarypos))
//					.equals(htblColNameValue.get(clusteringKey)); index--)
					.compareTo(htblColNameValue.get(clusteringKey))==0; index--)
				//TODO: did that for the Polygons thing; return to the first tuple with same area; not same coordinates
				;
			index++;
		}

		ArrayList<String> x = new ArrayList<>();
		for (int i = 0; i < metaOfTable.size(); i++) {
			x.add(metaOfTable.get(i)[1]);
		}
		for (int k = index; k <= Math.min(tuples.size()-1, lastOcc); k++) {
			Tuple t = tuples.get(k);
			if (validDelete(x, htblColNameValue, t)) {
				for (int i = 0; i < tuples.get(k).getAttributes().size() - 2; i++) {
					for (int j = 0; j < allIndices.size(); j++) {
						if (allIndices.get(j).equals(x.get(i))) {
							TreeIndex tree = colNameTreeIndex.get(allIndices.get(j));
							GeneralReference gr = tree.search((Comparable) t.getAttributes().get(i));
							if (gr instanceof Ref) {
								//System.out.println("asd");
								tree.delete((Comparable) tuples.get(k).getAttributes().get(i));
							} else {
								if(gr instanceof OverflowReference)
								{
									OverflowReference ofr = (OverflowReference) gr;
									{
									
											//System.out.println(ofr.getFirstPageName() + "  " + this.pageName + "  " + tree.toString()+ "  " +tuples.get(k).getAttributes().get(i));
											
//											deleteFromOverFlow(ofr, this.pageName, tree, t.getAttributes().get(i));
										
											tree.delete((Comparable)t.getAttributes().get(i),this.pageName );
											
											//System.out.println("hi");
											//System.out.println(ofr.getFirstPageName() + "  " + this.pageName + "  " + tree.toString()+ "  " +tuples.get(k).getAttributes().get(i));
										
									}
									
								}
								
								// System.out.println(this.pageName);
							}
						}
					}
				}
				// System.out.println(tuples.get(k).getAttributes().get(1));
				tuples.remove(k);
				// System.out.println(tuples.get(k).getAttributes().get(1));
				k--;

			}

		}

	}

	public static void deleteFromOverFlow(OverflowReference ofr, String pageName, TreeIndex tree, Object value)
			throws DBAppException {
		//System.out.println(ofr.getFirstPageName());
		//System.out.println("hamada");
		//System.out.println(ofr.getFirstPageName());
		File tmpDir = new File("data/" + ofr.getFirstPageName() + ".class");
		
		boolean exists = tmpDir.exists();
		if (exists) {
			//System.out.println(ofr.getFirstPageName());
			
			OverflowPage ofp = ofr.deserializeOverflowPage(ofr.getFirstPageName());
			//System.out.println("hamada1");
			for (int i = 0; i < ofp.getRefs().size(); i++) {
				if (ofp.getRefs().get(i).getPage().equals(pageName)) {
					ofp.getRefs().remove(i);
					//System.out.println("hello");
					if (ofp.getRefs().size() == 0) {
						//System.out.println("hello World");
						if (ofp.getNext() == null) {
							tree.delete((Comparable) value);
							//System.out.println("asdasdasdasd");
						} 
						else {
							//System.out.println("ayhaga");
							ofr.setFirstPageName(ofp.getNext());
							//TODO: reserialize the leaf
						}
							
						File f = new File("data/" + ofp.getPageName() + ".class");
						//System.out.println("deleted");
						System.out.println("/////||||\\\\\\\\\\\\\\\\\\deleting file "+ofp.getPageName());
						f.delete();
					} 
					else {
						ofp.serialize();
					}
					return;
				}
			}
				
				OverflowPage currOFP = ofp.deserialize(ofp.getNext());
				OverflowPage before = ofp;
				boolean notNull = true;
				while (notNull) {
					for (int i = 0; i < currOFP.getRefs().size(); i++) {
						if (currOFP.getRefs().get(i).getPage().equals(pageName)) {
							currOFP.getRefs().remove(i);
							if (currOFP.getRefs().size() == 0 && currOFP.getNext() != null) {
								before.setNext(currOFP.getNext());
								before.serialize();
								File f = new File("data/" + currOFP.getPageName() + ".class");
								System.out.println("/////||||\\\\\\\\\\\\\\\\\\deleting file "+currOFP.getPageName());
								f.delete();
								//notNull = false;
							} 
							if(currOFP.getRefs().size() == 0 && currOFP.getNext() == null)
							{
								before.setNext(null);
								before.serialize();
								File f = new File("data/" + currOFP.getPageName() + ".class");
								System.out.println("/////||||\\\\\\\\\\\\\\\\\\deleting file "+currOFP.getPageName());
								f.delete();
								//notNull = false;
							}
							else {
								currOFP.serialize();
								//before.serialize();
								//notNull = false;
							}
							return;
						}
					}
					if (currOFP.getNext() != null) {
						//System.out.println("asdads");
						//before.serialize();
						before = currOFP;
						//currOFP.serialize();
						currOFP = currOFP.deserialize(currOFP.getNext());
					} else {
						
						notNull = false;
					}
				}
			
		}
		else //recently removed
		{
			tree.delete((Comparable) value);
		}
	}

	public void deleteInPageWithBS(Hashtable<String, Object> htblColNameValue, Vector<String[]> metaOfTable,
			String clusteringKeyValue, int primaryPos, String clusteringKey) {

		int index = bsLastOcc((Comparable) htblColNameValue.get(clusteringKey), primaryPos);
		ArrayList<String> x = new ArrayList<>();
		for (int i = 0; i < metaOfTable.size(); i++) {
			x.add(metaOfTable.get(i)[1]);
		}
		for (int i = index; i >= 0; i--) {
			Tuple t = tuples.get(i);
			if (t.getAttributes().get(primaryPos).equals(htblColNameValue.get(clusteringKey))) {
				if (validDelete(x, htblColNameValue, t)) {
					// System.out.println("asdas");
					tuples.remove(i);
					i++;
				}
			}

		}

	}

	public boolean validDelete(ArrayList<String> x, Hashtable<String, Object> htblColNameValue, Tuple t) {
		Set<String> keys = htblColNameValue.keySet();
		ArrayList<String> y = new ArrayList<>();
		for (String key : keys) {
			y.add(key);
		}
		for (int i = 0; i < y.size(); i++) {
			for (int j = 0; j < x.size(); j++) {
				if (y.get(i).equals(x.get(j))) {
					if (!(htblColNameValue.get(y.get(i)).equals(t.getAttributes().get(j)))) {
						return false;
					}
				}
			}
		}
		return true;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (Object o : tuples) {
			Tuple x = (Tuple) o;
			sb.append(x.toString());
			// sb.append("#");
			sb.append("\n");
		}
		return sb.toString() + "\n";
	}
	/*
	 * public void insertIntoTVector(Tuple x, int m) { for (int i = m; i <
	 * tuples.size() - 1; i++) { Tuple c = tuples.get(i); tuples.insertElementAt(x,
	 * i); x = c; } tuples.addElement(x); }
	 */

	/*
	 * public void deleteInPage2(Object keyValue, int primaryPos) { for (int i = 0;
	 * i < tuples.size(); i++) { if
	 * (tuples.get(i).getAttributes().get(primaryPos).equals(keyValue)) {
	 * tuples.remove(i); } }
	 * 
	 * }
	 */

	/*
	 * public void deleteInPage(Object keyValue , int pos) { tuples.remove(pos);
	 * for(int i = pos; i < tuples.size() ; i++) {
	 * if(tuples.get(i).getAttributes().get(pos).equals(keyValue)) {
	 * tuples.remove(i); i--; } }
	 * 
	 * }
	 */

	/*
	 * public void deleteInPage(Object keyValue , int pos) { int lower = 0; int
	 * upper = tuples.size(); while(lower < upper) { int middle = (lower + upper)/2;
	 * if(tuples.get(middle).getAttributes().get(pos).equals(keyValue)) {
	 * tuples.remove(middle); } else {
	 * if(keyValue.toString().compareTo(tuples.get(middle).getAttributes().get(pos).
	 * toString())>0) //delete toString() lama nezabat compareto { lower = middle; }
	 * else { upper = middle; } } } }
	 */

	/*
	 * public static void main(String[] args) throws DBAppException { Page h = new
	 * Page(); Tuple abdo = new Tuple(); // Vector g = new Vector();
	 * abdo.addAttribute("mohsen"); abdo.addAttribute(2001); // abdo.z.add(true);
	 * 
	 * Tuple abdo2 = new Tuple(); // Vector l = new Vector();
	 * abdo2.addAttribute("shabra"); abdo2.addAttribute(1999); // abdo2.z.add(true);
	 * 
	 * h.tuples.add(abdo); h.tuples.add(abdo2); System.out.println(h.tuples);
	 * 
	 * try { FileOutputStream fileOut = new FileOutputStream("childSer3.ser");
	 * ObjectOutputStream out = new ObjectOutputStream(fileOut);
	 * out.writeObject(h.tuples); out.close(); fileOut.close();
	 * System.out.println("Serialized data is done");
	 * 
	 * FileInputStream fileIn = new FileInputStream("childSer3.ser");
	 * ObjectInputStream in = new ObjectInputStream(fileIn); Vector xx = (Vector)
	 * in.readObject(); in.close(); fileIn.close(); System.out.println(xx);
	 * 
	 * } catch (Exception e) { e.printStackTrace();
	 * 
	 * }
	 * 
	 * }
	 */
}
