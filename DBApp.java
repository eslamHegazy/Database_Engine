package DBV2;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.io.*;
import java.util.*;

//TABLES ARE SERIALIZED
//CHECK UPON INSERTION
public class DBApp {
	static Vector tables = new Vector();
	public void deleteFromTable(String strTableName,
			 Hashtable<String,Object> htblColNameValue) {
		//TODO
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

	public static Table getTable(String strTableName) {
		Table y = null;

		for (Object x : tables) {
			y = (Table) x;
			if (y.getTableName().equals(strTableName)) {
				break;
			}
		}
		return y;
	}

	public static void createTable(String strTableName, String strClusteringKey,
			Hashtable<String, String> htblColNameType) throws Exception {
		Table table = new Table();
		table.setTableName(strTableName);
		table.setStrClusteringKey(strClusteringKey);
		try (FileWriter writer = new FileWriter(new File("metadata.csv"), true)) {

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
		tables.add(table);

	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws Exception {
		Table y = getTable(strTableName);
		if (y == null) {
			System.err.println("NoSuchTable");
			return;
		}
		Object keyValue = null;
		Tuple newEntry = new Tuple();
		int i = 0;
		Vector meta = readFile("metadata.csv");
		for (Object O : meta) {
			String[] curr = (String[]) O;

			if (curr[0].equals(strTableName)) {
				String name = curr[1];
				String type = curr[2];
				if (!htblColNameValue.contains(name)) {
					System.err.println("col name invalid/missing column");
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

	}

	public void updateTable(String strTableName, String strKey, Hashtable<String, Object> htblColNameValue)
			throws Exception {
		Table t = getTable(strTableName);
		if (t == null) {
			System.err.println("NoSuchTable");
			return;
		}
		Vector meta = readFile("metadata.csv");
		int x = -1;
		Vector<String[]> metaOfTable=new Vector();
		for (Object o : meta) {
			String[] line = (String[]) o;
			if (line[0].equals(strTableName)) {
				if (Boolean.parseBoolean(line[3])) {
					String[] typeParts = (line[2]).split(".");
					if (typeParts[2].equals("Integer")) {
						x = 1;
					} else if (typeParts[2].equals("String")) {
						x = 2;
					} else {
						x = 3; // prim key is a double
					}
					
				}
				metaOfTable.add(line);
			}

		}
		t.updateTable(strKey,x,htblColNameValue,metaOfTable);
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException {
		/*
		 * String strTableName = "Student"; Hashtable htblColNameType = new Hashtable();
		 * htblColNameType.put("id", "java.lang.Integer"); htblColNameType.put("name",
		 * "java.lang.String"); htblColNameType.put("gpa", "java.lang.double");
		 * createTable(strTableName, "id", htblColNameType);
		 */
		Object x = new String("habdoo");
		String y = "java.lang.String";
		String strColType = y;
		Class colType = Class.forName(strColType);
		Class parameterType = x.getClass();
		if (!colType.equals(parameterType)) {
			System.err.println("DATA types 8alat");
		} else {
			System.out.println("LalO");
		}
	}
}
