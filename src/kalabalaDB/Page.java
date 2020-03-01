package kalabalaDB;
import java.io.*;
import java.util.*;
public class Page implements Serializable {
	private Vector<Tuple> tuples;
	private String pageName;
	private static  int lastIn = 0;

	public Page() {
		tuples = new Vector<Tuple>();
		pageName = "page" + lastIn;
		lastIn++;
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

	public void insertIntoTVector(Tuple x, int m) {
		for (int i = m; i < tuples.size() - 1; i++) {
			Tuple c = tuples.get(i);
			tuples.insertElementAt(x, i);
			x = c;
		}
		tuples.addElement(x);
	}
	
	public int binarySearch(Comparable key,int pos) {
		int ans = bsLastOcc(key, pos);
		if (ans==-1) {
			ans=bsFirstGreater(key, pos);
			if (ans==-1) {
				ans= tuples.size();
			}
		}
		return ans;
		
	}
	public int bsLastOcc(Comparable key, int pos) {
		int ans = -1;
		int lo = 0; 
		int hi = tuples.size()-1;
		int med = lo+(hi-lo+1)/2;
		while (lo<hi) {
			med = lo + (hi-lo+1)/2;
			Comparable curVal = (Comparable) tuples.get(med).getAttributes().get(pos);
			if (curVal.compareTo(key)<0) {
				lo = med+1;
			}
			else if (curVal.compareTo(key)==0) {
				lo = med+1;
				ans = med;
			}
			else {//curVal>med
				hi = med-1;
			}
		}
		return ans;
	}
	public int bsFirstGreater(Comparable key,int pos) {
		int ans = -1;
		int lo = 0; 
		int hi = tuples.size()-1;
		int med = lo+(hi-lo+1)/2;
		while (lo<=hi) {
			med = lo + (hi-lo+1)/2;
			Comparable curVal = (Comparable) tuples.get(med).getAttributes().get(pos);
			if (curVal.compareTo(key)<=0) {
				lo = med+1; 
			}
			else {//curVal>med
				ans = med;
				hi = med-1;
			}
		}
		return ans;
	}
	public void insertIntoPage(Tuple x, int pos) {
		Comparable nKey = (Comparable) x.getAttributes().get(pos);
		int lower = 0;
		int upper = tuples.size()-1;
		if (upper == -1) {
			tuples.addElement(x);
		} else {
//			int ans = -1;
			
			int ans = binarySearch(nKey,pos);
			if(ans==tuples.size()) {
				tuples.add(x);
			}
			else {
				tuples.insertElementAt(x, ans);
			}
		}
	}

	public void serialize() throws DBAppException {
		try {
//		System.out.println("###################################");
//		System.out.println("page before");
//		System.out.println(this);
		FileOutputStream fileOut = new FileOutputStream("data/"+pageName + ".class");
//		FileOutputStream fileOut = new FileOutputStream("data/"+pageName + ".ser");
		//TODO: Check resulting path
		//TODO: CHECK THIS SER/CLASS PROBLEM OF BOODY
		
		ObjectOutputStream out = new ObjectOutputStream(fileOut);
		out.writeObject(this);

		out.close();
		fileOut.close();
		}
		catch(IOException e) {
			throw new DBAppException("IO Exception");
		}
	}

	/*
	 * public void deleteInPage(Object keyValue , int pos) { tuples.remove(pos);
	 * for(int i = pos; i < tuples.size() ; i++) {
	 * if(tuples.get(i).getAttributes().get(pos).equals(keyValue)) {
	 * tuples.remove(i); i--; } }
	 * 
	 * }
	 */

	public void deleteInPage(Hashtable<String, Object> htblColNameValue, Vector<Integer> attributeIndex) {
		
		for (int i = 0; i < tuples.size(); i++) {
			Vector x = tuples.get(i).getAttributes(); // is it serialized?
			Set<String> keys = htblColNameValue.keySet();
			int j = 0;
			for (String key : keys) {
				// if(!x.get(attributeIndex.get(j)).equals(keys.g))

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

	/*public void deleteInPage2(Object keyValue, int primaryPos) {
		for (int i = 0; i < tuples.size(); i++) {
			if (tuples.get(i).getAttributes().get(primaryPos).equals(keyValue)) {
				tuples.remove(i);
			}
		}

	}*/

	/*
	 * public void deleteInPage(Object keyValue , int pos) { int lower = 0; int
	 * upper = tuples.size(); while(lower < upper) { int middle = (lower + upper)/2;
	 * if(tuples.get(middle).getAttributes().get(pos).equals(keyValue)) {
	 * tuples.remove(middle); } else {
	 * if(keyValue.toString().compareTo(tuples.get(middle).getAttributes().get(pos).
	 * toString())>0) //delete toString() lama nezabat compareto { lower = middle; }
	 * else { upper = middle; } } } }
	 */

	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (Object o : tuples) {
			Tuple x = (Tuple) o;
			sb.append(x.toString());
//			sb.append("#");
			sb.append("\n");

		}
		return sb.toString()+"\n";
	}
	public static void main(String[] args) throws DBAppException {
		Page h = new Page();
		Tuple abdo = new Tuple();
		// Vector g = new Vector();
		abdo.addAttribute("mohsen");
		abdo.addAttribute(2001);
		// abdo.z.add(true);

		Tuple abdo2 = new Tuple();
		// Vector l = new Vector();
		abdo2.addAttribute("shabra");
		abdo2.addAttribute(1999);
		// abdo2.z.add(true);

		h.tuples.add(abdo);
		h.tuples.add(abdo2);
		System.out.println(h.tuples);

		try {
			FileOutputStream fileOut = new FileOutputStream("childSer3.ser");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(h.tuples);
			out.close();
			fileOut.close();
			System.out.println("Serialized data is done");

			FileInputStream fileIn = new FileInputStream("childSer3.ser");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			Vector xx = (Vector) in.readObject();
			in.close();
			fileIn.close();
			System.out.println(xx);

		} catch (Exception e) {
			e.printStackTrace();

		}

	}

}
