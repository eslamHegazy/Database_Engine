package kalabalaDB;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DBApp {
	// static Vector tables=new Vector();
//	public static Vector<String> tables = new Vector();
	int MaximumRowsCountinPage ;
	
	public static void clear() {
		File metadata = new File("data/metadata.csv");
		metadata.delete();
		File path = new File("data/");
		for (String filename : path.list()) {
			File fileToBeDeleted = new File("data/"+filename);
			fileToBeDeleted.delete();
		}
	}

	public void init() throws DBAppException{
		try {
			InputStream inStream = new FileInputStream("config/DBApp.properties");
			Properties bal = new Properties();
			bal.load(inStream);
//			Set s = bal.keySet();
			MaximumRowsCountinPage = Integer.parseInt(bal.getProperty("MaximumRowsCountinPage"));
		}
		catch(IOException e) {
			System.out.println(e.getStackTrace());
			throw new DBAppException("IO Exception");
		}
		

	}
	
	public Vector<String> getTablesNames()throws DBAppException{
		Vector meta = readFile("data/metadata.csv");
		Vector res = new Vector<>();
		HashSet<String> hs = new HashSet<>();
		for (Object O : meta) {
			String[] curr = (String[]) O;
			//TODO: If added the headers row to metadata.csv ;;
			// will need to discard the first row
			if (hs.contains(curr[0]))
				continue;
			else {
				res.add(curr[0]);
				hs.add(curr[0]);
			}
		}
		return res;
	}
	public void printAllPagesInAllTables() throws DBAppException{
		printAllPagesInAllTables("AllData");
	}
	public void printAllPagesInAllTables(String fileName) throws DBAppException {
		try {
		File file = new File("data/"+fileName+".txt");
		
		FileWriter yy = new FileWriter(file);
		PrintWriter writeFile = new PrintWriter(yy);
		Vector<String> tables = getTablesNames();
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
		catch (IOException e) {
			throw new DBAppException("IO Exception");
		}
	}
	public boolean exists(String strTableName) throws DBAppException{
		try {
			Vector meta = readFile("data/metadata.csv");
			for (Object O : meta) {
				String[] curr = (String[]) O;
				if (curr[0].equals(strTableName)) {
					return true;
				}
			}
			return false;
		}
		catch (Exception e) {
			return false;
		}
	
	}
	public void createTable(String strTableName, String strClusteringKey, Hashtable<String, String> htblColNameType)
			throws DBAppException {
		if (exists(strTableName)) {
			throw new DBAppException("A table with this name already exists in the database!");
		}
		Table table = new Table();
		table.setMaximumRowsCountinPage(MaximumRowsCountinPage);
		table.setTableName(strTableName);
		table.setStrClusteringKey(strClusteringKey);
		try (FileWriter writer = new FileWriter(new File("data/metadata.csv"), true)) {

			Set<String> keys = htblColNameType.keySet();
			for (String key : keys) {
//				System.out.println("Value of " + key + " is: " + htblColNameType.get(key));
				writer.append(strTableName + ",");
				writer.append(key + ",");
				String typ = htblColNameType.get(key);
				writer.append(htblColNameType.get(key) + ",");
				writer.write((strClusteringKey.equals(key)) ? "True," : "False,");
//				writer.write("False" + ",");
				writer.write("False");
				writer.write("\n");
			}

		}
		catch (IOException e) {
			throw new DBAppException("IO Exception");
		}
		serialize(table);
//		tables.add(strTableName);
	}
	
	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
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
					throw new DBAppException("col name invalid");
				} else {
					// String strColType=(String) colTypes.get(i++);
					try {
						Class colType = Class.forName(type);
						Class parameterType = htblColNameValue.get(name).getClass();
						// System.out.println(colType+" "+parameterType);
						Class polyOriginal = Class.forName("java.awt.Polygon");
						if (colType == polyOriginal) {
							colType = Class.forName("kalabalaDB.Polygons");
						}
						if (!colType.equals(parameterType)) {
							throw new DBAppException("DATA types 8alat");
						} else {
							newEntry.addAttribute(htblColNameValue.get(name));
							if (Boolean.parseBoolean(curr[3])) {
								y.setPrimaryPos(i);
								keyValue = htblColNameValue.get(name);
	
							}
						}
					} 
					catch(ClassNotFoundException e) {
						throw new DBAppException("Class Not Found Exception");
					}
					
				}
				i++;
			}
		}
		
		newEntry.addAttribute(new Date());
		y.insertSorted(newEntry, keyValue); // TODO
		serialize(y);

	}

	public void updateTable(String strTableName, String strClusteringKey, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {
		Table y = deserialize(strTableName);
		try {
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
						key = parseDate(strClusteringKey);
					else if (curr[2].equals("java.lang.Boolean"))
						key = Boolean.parseBoolean(strClusteringKey);
					else if (curr[2].equals("java.awt.Polygon"))
						key = Polygons.parsePolygon(strClusteringKey);
					else
						throw new DBAppException("The key has an UNKNOWN TYPE");
						//TODO: Is the previous line good ? 
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
				if (!colnames.contains(str)) {
//					System.out.println("Invalid column types");
					throw new DBAppException("Invalid column types");
//					return;
				}
				int pos = colnames.indexOf(str);
				Class colType = Class.forName(types.get(pos));
				Class parameterType = htblColNameValue.get(str).getClass();
				Class polyOriginal = Class.forName("java.awt.Polygon");
				if (colType == polyOriginal) {
					colType = Class.forName("kalabalaDB.Polygons");
				}
				System.out.println(colType+" "+parameterType);
				if (!colType.equals(parameterType)) {
					throw new DBAppException("Data types do not match with those of the actual column of the table");
				}
			}

		
		
			String[] searchResult = SearchInTable(strTableName, strClusteringKey).split("#");
			Page p = Table.deserialize(searchResult[0]);
			int i = Integer.parseInt(searchResult[1]);
			int j = y.getPages().indexOf(searchResult[0]);
			boolean flag = true;
			while (j < y.getPages().size() && flag) {
				p = Table.deserialize(y.getPages().get(j));
				
				while (i < p.getTuples().size()&& flag) {
					Tuple current = p.getTuples().get(i);
	
					if (!current.getAttributes().get(y.getPrimaryPos()).equals(key)) {
						flag = false;
						break;
					}
	
					for (int k = 0; k < current.getAttributes().size()-1; k++) {
						System.out.printf("k=%d, %s\n",k,colnames.get(k));
						if (htblColNameValue.containsKey(colnames.get(k))) {
							System.out.println(k+", before :"+current);
							current.getAttributes().setElementAt(htblColNameValue.get(colnames.get(k)), k);
							System.out.println(k+", after :"+current);
						}
						
					}
					Date date = new Date();
					current.getAttributes().setElementAt(date, current.getAttributes().size()-1);
	
					i++;
				}
				j++;
				i = 0;
				System.out.println("page after: "+p);
				p.serialize();
			}
		}
		catch(ClassNotFoundException e) {
			throw new DBAppException("Class not found Exception");
		}
		serialize(y);
	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		/*
		 * Table y = null; Object keyValue = null; for (Object x : tables) { y = (Table)
		 * x; if (y.getTableName().equals(strTableName)) { break; } } if (y == null) {
		 * System.err.println("NoSuchTable"); return; }
		 */
		Table y = deserialize(strTableName);
		Vector meta = readFile("data/metadata.csv");
		Vector<String[]> metaOfTable = new Vector();
		for (Object o : meta) {
			String[] line = (String[]) o;
			if (line[0].equals(strTableName)) {
				metaOfTable.add(line);
			}
		}
		
		y.deleteInTable(htblColNameValue, metaOfTable);
		serialize(y);
		/*
		 * String x = SearchInTable(strTableName, (String)keyValue); String[] arrOfStr =
		 * x.split("#"); String pName =arrOfStr[0]; int tuplePosition =
		 * Integer.parseInt(arrOfStr[1]); y.deleteInTable(keyValue , pName ,
		 * tuplePosition);
		 */
		
	}

	public static void serialize(Table table) throws DBAppException {
		try {
			FileOutputStream fileOut = new FileOutputStream("data/"+table.getTableName() + ".class");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(table);
			out.close();
			fileOut.close();
		}
		catch(IOException e) {
			throw new DBAppException("IO Exception");
		}
	}

	public static Table deserialize(String tableName) throws DBAppException {
		try {
			FileInputStream fileIn = new FileInputStream("data/"+tableName + ".class");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			
			Table xx = (Table) in.readObject();
			in.close();
			fileIn.close();
			
			return xx;
		}
		catch (IOException e) {
			e.printStackTrace();
			
			throw new DBAppException("IO Exception | Probably wrong table name (tried to operate on a table that does not exist !");
			//TODO: Fix the above line :D
		}
		catch (ClassNotFoundException e) {
			throw new DBAppException("Class Not Found Exception");
			//TODO: Fix the above line :D
		}
	}

	public static Vector readFile(String path) throws DBAppException {
		try {
			String currentLine = "";
			FileReader fileReader = new FileReader(path);
			BufferedReader br = new BufferedReader(fileReader);
			Vector metadata = new Vector();
			while ((currentLine = br.readLine()) != null) {
				metadata.add(currentLine.split(","));
			}
			return metadata;
		}
		catch(IOException e) {
			e.printStackTrace();
			throw new DBAppException("IO Exception");
		}
	}


	public String SearchInTable(String strTableName, String strKey) throws DBAppException {
		/*
		 * Table y = null; Object keyValue = null; for (Object x : tables) { y = (Table)
		 * x; if (y.getTableName().equals(strTableName)) { break; } } if (y == null) {
		 * System.err.println("NoSuchTable"); return "-1"; }
		 */
		try {
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
					else if (curr[2].equals("java.awt.Polygon"))
						key = (Comparable) Polygons.parsePolygon(strKey);
					else {
//						TODO:return "-1";
						throw new DBAppException("Searching for a key of unknown type !");
					}
				}
			}
	
			Table t = deserialize(strTableName);
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
	
	static Date parseDate(String strClusteringKey) throws DBAppException {
		try {
			SimpleDateFormat s = new SimpleDateFormat("yyyy-MM-dd");
			Date d = s.parse(strClusteringKey);
			return d;
		}
		catch (ParseException e) {
			throw new DBAppException("Parse Exception : Entered a wrong date format");
		}
	}
	

//	public static void main(String[] args) throws DBAppException {
//		clear();
////		/*
//		String strTableName = "Student";
//		DBApp dbApp = new DBApp();
//		Hashtable htblColNameType = new Hashtable();
//		htblColNameType.put("id", "java.lang.Integer");
//		htblColNameType.put("name", "java.lang.String");
//		htblColNameType.put("gpa", "java.lang.Double");
//		dbApp.createTable(strTableName, "id", htblColNameType);
//		 System.out.println("hii 1");
//		for (int i = 1; i <= 250; i += 2) {
//			Hashtable htblColNameValue = new Hashtable();
//			htblColNameValue.put("id", new Integer(i));
//			htblColNameValue.put("name", new String("Ahmed Noor"));
//			htblColNameValue.put("gpa", new Double(0.95));
//			dbApp.insertIntoTable(strTableName, htblColNameValue);
//		}
//		 System.out.println("hii 2");
//		dbApp.printAllPagesInAllTables();
//		for (int i = 2; i <= 250; i += 2) {
//			// System.out.println("hii"+i);
//			Hashtable htblColNameValue = new Hashtable();
//			htblColNameValue.put("id", new Integer(i));
//			htblColNameValue.put("name", new String("Ahmed Noor"));
//			htblColNameValue.put("gpa", new Double(0.95));
//			dbApp.insertIntoTable(strTableName, htblColNameValue);
//		}
//		 System.out.println("hii 3");
//		dbApp.printAllPagesInAllTables();
//		for (int i = 0; i < 250; i++) {
//			Hashtable htblColNameValue = new Hashtable();
//			htblColNameValue.put("id", new Integer(200));
//			htblColNameValue.put("name", new String("Ahmed Noor"));
//			htblColNameValue.put("gpa", new Double(0.95));
//			dbApp.insertIntoTable(strTableName, htblColNameValue);
//		}
//		System.out.println("hii 4");
//		dbApp.printAllPagesInAllTables();
////		 */
//	}

	
//	public static void main(String[] args) throws IOException{
//		Properties properties = new Properties();
//		properties.setProperty("MaximumRowsCountinPage", "200");
//		properties.setProperty("NodeSize", "15");
//		OutputStream out = new FileOutputStream("config/DBApp.properties");
//		properties.store(out, "");
//		InputStream inStream = new FileInputStream("config/DBApp.properties");
//		Properties bal = new Properties();
//		bal.load(inStream);
//		Set s = bal.keySet();
//		for (Object x : s) {
//			String str = (String) x;
//			System.out.println(str+" "+bal.getProperty(str));
//		}
//	}
}