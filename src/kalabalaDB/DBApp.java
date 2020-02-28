package kalabalaDB;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.awt.Polygon;
import java.io.*;
import java.util.*;

public class DBApp {
	// static Vector tables=new Vector();
	public static Vector<String> tables = new Vector();

	public static void clear() {
		File metadata = new File("data/metadata.csv");
		metadata.delete();
		File path = new File("data/");
		for (String filename : path.list()) {
			File fileToBeDeleted = new File("data/"+filename);
			fileToBeDeleted.delete();
		}
	}

	public void init() {
	}

	public void printAllPagesInAllTables() throws ClassNotFoundException, IOException {
		File file = new File("data/AllData.txt");
		FileWriter yy = new FileWriter(file);
		PrintWriter writeFile = new PrintWriter(yy);
		for (String tblName : tables) {
			writeFile.println("Table " + tblName + " has the following pages:\n\n");
			Table y = deserialize(tblName);
			for (String pageName : y.getPages()) {
				writeFile.println("Page " + pageName + " has the following tuples:\n");
				Page p = Table.deserialize(pageName);
				writeFile.println(p);
				p.serialize();
			}
			writeFile.println("\n\n\n");
			serialize(y);
		}
		writeFile.close();

	}

	public void createTable(String strTableName, String strClusteringKey, Hashtable<String, String> htblColNameType)
			throws Exception {
		Table table = new Table();
		table.setTableName(strTableName);
		table.setStrClusteringKey(strClusteringKey);
		try (FileWriter writer = new FileWriter(new File("data/metadata.csv"), true)) {

			Set<String> keys = htblColNameType.keySet();
			for (String key : keys) {
				System.out.println("Value of " + key + " is: " + htblColNameType.get(key));
				writer.append(strTableName + ",");
				writer.append(key + ",");
				writer.append(htblColNameType.get(key) + ",");
				writer.write((strClusteringKey.equals(key)) ? "True," : "False,");
				writer.write("False" + ",");
				writer.write("\n");
			}

		} catch (FileNotFoundException e) {
			System.out.println(e.getMessage());
		}

		serialize(table);
		tables.add(strTableName);

	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws Exception {
		/*
		 * Table y = null;
		 * 
		 * for (Object x : tables) { y = (Table) x; if
		 * (y.getTableName().equals(strTableName)) { break; } } if (y == null) {
		 * System.err.println("NoSuchTable"); return; }
		 */
		boolean found = false;
		for (String y : tables) {
			if (y.equals(strTableName)) {
				found = true;
				break;
			}
		}
		if (!found) {
			System.err.println("NoSuchTable");
			return;
		}
		Table y = deserialize(strTableName);
		Object keyValue = null;
		Tuple newEntry = new Tuple();
		int i = 0;
		Vector meta = readFile("data/metadata.csv");
		for (Object O : meta) {
			String[] curr = (String[]) O;

			if (curr[0].equals(strTableName)) {
				String name = curr[1];
				String type = curr[2];
				if (!htblColNameValue.containsKey(name)) {
					System.err.println("col name invalid");
					return;
				} else {
					// String strColType=(String) colTypes.get(i++);
					Class colType = Class.forName(type);
					Class parameterType = htblColNameValue.get(name).getClass();
					if (!colType.equals(parameterType)) {
						System.err.println("DATA types 8alat");
						return;
					} else {
						newEntry.addAttribute(htblColNameValue.get(name));
						if (Boolean.parseBoolean(curr[3])) {
							y.setPrimaryPos(i);
							keyValue = htblColNameValue.get(name);

						}
					}
				}
				i++;
			}
		}
		y.insertSorted(newEntry, keyValue); // TODO
		serialize(y);

	}

	public void updateTable(String strTableName, String strClusteringKey, Hashtable<String, Object> htblColNameValue)
			throws Exception {
		boolean found = false;
		for (String y : tables) {
			if (y.equals(strTableName)) {
				found = true;
				break;
			}
		}
		if (!found) {
			System.err.println("NoSuchTable");
			return;
		}
		Table y = deserialize(strTableName);
		Vector meta = readFile("data/metadata.csv");
		Comparable key = null;
		for (Object O : meta) {
			String[] curr = (String[]) O;
			if (curr[0].equals(strTableName) && curr[3].equals("True")) // search in metadata for the table name and the
																		// key
			{
				if (curr[2].equals("java.lang.Integer"))
					key = Integer.parseInt(strClusteringKey);
				else if (curr[2].equals("java.lang.Double"))
					key = Double.parseDouble(strClusteringKey);
				else if (curr[2].equals("java.util.Date"))
					key = Date.parse(strClusteringKey);
				else if (curr[2].equals("java.lang.Boolean"))
					key = Boolean.parseBoolean(strClusteringKey);
				else if (curr[2].equals(" java.awt.Polygon"))
					key = (Comparable) parsePolygon(strClusteringKey);
				else
					return;
			}
		}

		ArrayList<String> types = new ArrayList<String>();
		ArrayList<String> colnames = new ArrayList<String>();

		for (Object O : meta) {
			String[] curr = (String[]) O;
			if (curr[0].equals(strTableName)) // search in metadata for the table name and the key
			{
				String name = curr[1];
				String type = curr[2];
				types.add(type);
				colnames.add(name);
			}
		}
		// check validity of the hashtable entries
		Set<String> hashtableKeys = htblColNameValue.keySet();
		for (String str : hashtableKeys) {
			if (!htblColNameValue.contains(str)) {
				System.out.println("Invalid column types");
				return;
			}
			int pos = colnames.indexOf(str);
			Class colType = Class.forName(types.get(pos));
			Class parameterType = htblColNameValue.get(str).getClass();
			if (!colType.equals(parameterType)) {
				System.err.println("DATA types not Valid");
				return;
			}
		}

		String[] searchResult = SearchInTable(strTableName, strClusteringKey).split("#");
		Page p = Table.deserialize(searchResult[0]);
		int i = Integer.parseInt(searchResult[1]);
		int j = y.getPages().indexOf(searchResult[0]);

		while (j < y.getPages().size()) {
			p = Table.deserialize(y.getPages().get(j));
			while (i < p.getTuples().size()) {
				Tuple current = p.getTuples().get(i);

				if (!current.getAttributes().get(y.getPrimaryPos()).equals(key)) {
					return;
				}

				for (int k = 0; k < current.getAttributes().size(); k++) {
					if (htblColNameValue.contains(colnames.get(k))) {
						current.getAttributes().setElementAt(htblColNameValue.get(colnames.get(k)), k);
					}
				}

				i++;
			}
			j++;
			i = 0;
			p.serialize();
		}
		serialize(y);
	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws Exception {
		/*
		 * Table y = null; Object keyValue = null; for (Object x : tables) { y = (Table)
		 * x; if (y.getTableName().equals(strTableName)) { break; } } if (y == null) {
		 * System.err.println("NoSuchTable"); return; }
		 */
		boolean found = false;
		for (String y : tables) {
			if (y.equals(strTableName)) {
				found = true;
				break;
			}
		}
		if (!found) {
			System.err.println("NoSuchTable");
			return;
		}
		Vector meta = readFile("data/metadata.csv");
		Vector<String[]> metaOfTable = new Vector();
		for (Object o : meta) {
			String[] line = (String[]) o;
			if (line[0].equals(strTableName)) {
				metaOfTable.add(line);
			}
		}
		Table y = deserialize(strTableName);
		y.deleteInTable(htblColNameValue, metaOfTable);
		serialize(y);
		/*
		 * String x = SearchInTable(strTableName, (String)keyValue); String[] arrOfStr =
		 * x.split("#"); String pName =arrOfStr[0]; int tuplePosition =
		 * Integer.parseInt(arrOfStr[1]); y.deleteInTable(keyValue , pName ,
		 * tuplePosition);
		 */

	}

	public static void serialize(Table table) throws IOException {
		FileOutputStream fileOut = new FileOutputStream(table.getTableName() + ".class");
		ObjectOutputStream out = new ObjectOutputStream(fileOut);
		out.writeObject(table);
		out.close();
		fileOut.close();
	}

	public static Table deserialize(String tableName) throws IOException, ClassNotFoundException {
		FileInputStream fileIn = new FileInputStream(tableName + ".class");
		ObjectInputStream in = new ObjectInputStream(fileIn);
		Table xx = (Table) in.readObject();
		in.close();
		fileIn.close();
		return xx;
	}

	public static Vector readFile(String path) throws IOException {
		String currentLine = "";
		FileReader fileReader = new FileReader(path);
		BufferedReader br = new BufferedReader(fileReader);
		Vector metadata = new Vector();
		while ((currentLine = br.readLine()) != null) {
			metadata.add(currentLine.split(","));
		}
		return metadata;
	}

	public static String PolygonToString(Polygon P) {
		String str = "";
		String x = "{";
		for (int i = 0; i < P.npoints; i++) {
			x += P.xpoints[i];
			if (i != P.npoints - 1)
				x += "+";
		}
		x += "}";

		String y = "{";
		for (int i = 0; i < P.npoints; i++) {
			y += P.ypoints[i];
			if (i != P.npoints - 1)
				y += "+";
		}
		y += "}";

		str = "" + x + " " + y + " " + P.npoints;
		return str;
	}

	public static Polygon parsePolygon(String str) {
		String[] pol = str.split(" ");
		int npoint = Integer.parseInt(pol[2]);
		String[] strX = (pol[0].substring(1, pol[0].length() - 1)).split("+");
		String[] strY = (pol[1].substring(1, pol[1].length() - 1)).split("+");

		int[] x = new int[npoint];
		int[] y = new int[npoint];

		for (int i = 0; i < npoint; i++) {
			x[i] = Integer.parseInt(strX[i]);
			y[i] = Integer.parseInt(strY[i]);
		}

		return new Polygon(x, y, npoint);

	}

	public String SearchInTable(String strTableName, String strKey) throws Exception {
		/*
		 * Table y = null; Object keyValue = null; for (Object x : tables) { y = (Table)
		 * x; if (y.getTableName().equals(strTableName)) { break; } } if (y == null) {
		 * System.err.println("NoSuchTable"); return "-1"; }
		 */

		Vector meta = readFile("data/metadata.csv");
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
				else if (curr[2].equals(" java.awt.Polygon"))
					key = (Comparable) parsePolygon(strKey);
				else
					return "-1";
			}
		}

		Table t = deserialize(strTableName);
		Vector<String> pages = t.getPages();
		// Vector<String> MinMax = t.getMin().toString() ;

		for (String s : pages) {
			Page p = Table.deserialize(s);
			int l = 0;
			int r = p.getTuples().size();

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
					l = m + 1;

				// If x is smaller, ignore right half
				else
					r = m - 1;
			}
			p.serialize(); // added by abdo
		}
		serialize(t); // addd by abdo

		return "-1";
	}

	public static void main(String[] args) throws Exception {
		clear();
//		/*
		String strTableName = "Student";
		DBApp dbApp = new DBApp();
		Hashtable htblColNameType = new Hashtable();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.Double");
		dbApp.createTable(strTableName, "id", htblColNameType);
		 System.out.println("hii 1");
		for (int i = 1; i <= 250; i += 2) {
			Hashtable htblColNameValue = new Hashtable();
			htblColNameValue.put("id", new Integer(i));
			htblColNameValue.put("name", new String("Ahmed Noor"));
			htblColNameValue.put("gpa", new Double(0.95));
			dbApp.insertIntoTable(strTableName, htblColNameValue);
		}
		 System.out.println("hii 2");
		dbApp.printAllPagesInAllTables();
		for (int i = 2; i <= 250; i += 2) {
			// System.out.println("hii"+i);
			Hashtable htblColNameValue = new Hashtable();
			htblColNameValue.put("id", new Integer(i));
			htblColNameValue.put("name", new String("Ahmed Noor"));
			htblColNameValue.put("gpa", new Double(0.95));
			dbApp.insertIntoTable(strTableName, htblColNameValue);
		}
		 System.out.println("hii 3");
		dbApp.printAllPagesInAllTables();
		for (int i = 0; i < 250; i++) {
			Hashtable htblColNameValue = new Hashtable();
			htblColNameValue.put("id", new Integer(200));
			htblColNameValue.put("name", new String("Ahmed Noor"));
			htblColNameValue.put("gpa", new Double(0.95));
			dbApp.insertIntoTable(strTableName, htblColNameValue);
		}
		System.out.println("hii 4");
		dbApp.printAllPagesInAllTables();
//		 */
	}
	
}