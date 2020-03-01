package kalabalaDB;
import java.io.*;
import java.util.*;

public class Table implements Serializable {
	private Vector<String> pages = new Vector();
	private int MaximumRowsCountinPage;
	private Vector<Object>min=new Vector<>();
	private Vector<Object>max=new Vector<>();
	private String tableName;
	private String strClusteringKey;
	private int primaryPos;

	public Vector<String> getPages() {
		return pages;
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

	public void addInPage(int curr, Tuple x) throws DBAppException {
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
				p.serialize();
			} else {
				// Tuple t=p.getTuples().get(p.size()-1);//element 199
				p.insertIntoPage(x, primaryPos);
				Tuple t = p.getTuples().remove(p.size() - 1);
				Object minn = p.getTuples().get(0).getAttributes().get(primaryPos);
				Object maxx = p.getTuples().get(p.size() - 1).getAttributes().get(primaryPos);
				min.remove(curr);
				addInVector(min, minn, curr);
				max.remove(curr);
				addInVector(max, maxx, curr);
				p.serialize();
				addInPage(curr + 1, t);
			}
		} else {
			Page p = new Page();
			p.insertIntoPage(x, primaryPos);
			Object keyValue = p.getTuples().get(0).getAttributes().get(primaryPos);
			pages.addElement(p.getPageName());
			min.addElement(keyValue);
			max.addElement(keyValue);
			p.serialize();

		}

	}

	public void insertSorted(Tuple x, Object keyV) throws DBAppException{
		int lower = 0;
		int upper = min.size();
		Comparable keyValue=(Comparable) keyV;
		int curr=0;
		for(curr=0;curr<pages.size();curr++){
			Object minn=(min.get(curr));
			Object maxx=max.get(curr);
			if((keyValue.compareTo(minn)>=0&&keyValue.compareTo(maxx)<=0)||(keyValue.compareTo(minn)<0)||curr==pages.size()-1){
				addInPage(curr, x);
				break;
			}
		}
		if(pages.size()==0){
			Page p=new Page();
			p.insertIntoPage(x, primaryPos);
			pages.addElement(p.getPageName());
			min.addElement(keyV);
			max.addElement(keyV);	
			p.serialize();

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
				return true;

		}
		return false;

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

//		if (invalidDelete(htblColNameValue, metaOfTable)) {
//			throw new DBAppException("false operation");
//			//TODO: Is this message appropriate?
//		}
		Vector<Integer> attributeIndex = new Vector();
		Set<String> keys = htblColNameValue.keySet();
		for (String key : keys) {
			int i;
			for (i = 0; i < metaOfTable.size(); i++) {
				if (metaOfTable.get(i)[1].equals(key)) {
					break;
				}
			}
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

}
