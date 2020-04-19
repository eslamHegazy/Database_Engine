package kalabalaDB;


import java.awt.Polygon;
import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import BPTree.BPTree;
import General.GeneralReference;
import General.Ref;
import General.TreeIndex;
import RTree.RTree;

public class DBApp {
	// static Vector tables=new Vector();
//	public static Vector<String> tables = new Vector();
	int MaximumRowsCountinPage ;
	int nodeSize;
	public static void clear() {
		File metadata = new File("data/metadata.csv");
		if (metadata!=null) {
			System.out.println("/////||||\\\\\\\\\\\\\\\\\\deleting metadata");
			metadata.delete();
		}
		File data = new File("data/");
		String[] pages = data.list();
		if (pages==null) return;
		for (String p: pages) {
			File pageToDelete = new File("data/"+p);
			System.out.println("/////||||\\\\\\\\\\\\\\\\\\deleting file "+p);
			pageToDelete.delete();
		}
	}

	public void init() throws DBAppException{
		try {
			//Configuration 
			InputStream inStream = new FileInputStream("config/DBApp.properties");
			Properties bal = new Properties();
			bal.load(inStream);
			MaximumRowsCountinPage = Integer.parseInt(bal.getProperty("MaximumRowsCountinPage"));
			nodeSize=Integer.parseInt(bal.getProperty("NodeSize"));
			
			//Assuring data folder and metadata.csv exist
			File data = new File("data");
			data.mkdir();
			File metadata = new File("data/metadata.csv");
			metadata.createNewFile();
			File metaBPtree = new File("data/metaBPtree.csv");
			if(metaBPtree.createNewFile()) {
				FileWriter csvWriter = new FileWriter("data/metaBPtree.csv");
				csvWriter.append("0");
				csvWriter.flush();
				csvWriter.close();
			}
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
	
	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException {
		System.out.println("||||\t\tStart Inserting\t\t||||");
		Table y = deserialize(strTableName);
		Object keyValue = null;
		Tuple newEntry = new Tuple();
		String keyType="";
		String keyColName="";
		ArrayList colNames=new ArrayList<String>();
		int i = 0;
		Vector meta = readFile("data/metadata.csv");
		for (Object O : meta) {
			String[] curr = (String[]) O;
			if (curr[0].equals(strTableName)) {
				String name = curr[1];
				String type = curr[2];
				colNames.add(name);
				if (!htblColNameValue.containsKey(name)) {
					throw new DBAppException("col name invalid");
				} else {
					// String strColType=(String) colTypes.get(i++);
					try {
						Class colType = Class.forName(type);
						Class parameterType = htblColNameValue.get(name).getClass();
						Class polyOriginal = Class.forName("java.awt.Polygon");
						if (colType == polyOriginal) {
							Polygons p = new Polygons((Polygon)htblColNameValue.get(name));
							htblColNameValue.put(name, p);
						}
						if (!colType.equals(parameterType)) {
							throw new DBAppException("DATA types 8alat");
						} else {
							newEntry.addAttribute(htblColNameValue.get(name));
							if (Boolean.parseBoolean(curr[3])) {
								y.setPrimaryPos(i);
								keyValue = htblColNameValue.get(name);
								keyType=type;
								keyColName=name;
	
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
		int id = y.getLastID(true);	//please check getLastId body before copying 
		newEntry.addAttribute(id);
		y.insertSorted(newEntry, keyValue,keyType,keyColName,nodeSize,colNames); // TODO
		serialize(y);

		System.out.println("||||\t\tEnd Inserting\t\t||||");
	}

	public void updateTable(String strTableName, String strClusteringKey, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, IOException
	{
		System.out.println("||||\t\tStart Updating\t\t||||");
		Table y = deserialize(strTableName);
		try {
			Vector meta = readFile("data/metadata.csv");
			Comparable key = null;
			boolean key_index = false;
			String key_column_name = "";
			// get the key from the metadata and type cast it
			for (Object O : meta) 
			{
				String[] curr = (String[]) O;
				if (curr[0].equals(strTableName) && curr[3].equals("True")) // search in metadata for the table name and the													// key
				{
					key_column_name = curr[1];
					if(curr[4].equals("True"))
						key_index = true;
					
					if (curr[2].equals("java.lang.Integer"))
						key = Integer.parseInt(strClusteringKey);
					else if (curr[2].equals("java.lang.Double"))
						key = Double.parseDouble(strClusteringKey);
					else if (curr[2].equals("java.util.Date"))
						key = parseDate(strClusteringKey);
					else if (curr[2].equals("java.lang.Boolean"))
						key = Boolean.parseBoolean(strClusteringKey);
					else if (curr[2].equals("java.awt.Polygon"))
						key = Polygons.parsePolygons(strClusteringKey);
					else
						throw new DBAppException("The key has an UNKNOWN TYPE");
						//TODO: Is the previous line good ? 
				}
			}

			// get the full information about the table
			ArrayList<String> types = new ArrayList<String>();		// types of the columns
			ArrayList<String> colnames = new ArrayList<String>(); 	// hold the names of the columns
			ArrayList<Boolean> indexed = new ArrayList<Boolean>();	// hold if the column is indexed or not
			
			// get the full information about the table
			for (Object O : meta) 
			{
				String[] curr = (String[]) O;
				if (curr[0].equals(strTableName)) // search in metadata for the table name and the key
				{
					String name = curr[1];
					String type = curr[2];
					boolean indx = Boolean.parseBoolean(curr[4]);
					types.add(type);
					colnames.add(name);
					indexed.add(indx);
				}
			}
			
			// check validity of the hashtable entries
			Set<String> hashtableKeys = htblColNameValue.keySet();
			for (String str : hashtableKeys) 
			{
				if (!colnames.contains(str)) 
				{
//					System.out.println("Invalid column types");
					throw new DBAppException("Invalid column types");
//					return;
				}
				
				int pos = colnames.indexOf(str);
				Class colType = Class.forName(types.get(pos));
				Class parameterType = htblColNameValue.get(str).getClass();
				Class polyOriginal = Class.forName("java.awt.Polygon");
				
				if (colType.equals(polyOriginal)) {
					//TODO: Eslam: I think this is true  here only; NEED TO TEST IT
					//colType = Class.forName("kalabalaDB.Polygons");
					htblColNameValue.put(str, new Polygons((Polygon) htblColNameValue.get(str)));
				}
//				System.out.println(colType+" "+parameterType);
				
				if (!colType.equals(parameterType)) {
					throw new DBAppException("Data types do not match with those of the actual column of the table");	
				}
			}		
			
			
			// if the key is indexed use tree index
			if(key_index)
			{
				System.out.println("INDEX USED");
				TreeIndex key_tree = y.getColNameBTreeIndex().get(key_column_name);
				GeneralReference GR = key_tree.search(key);  // the result of the search in the B+ tree
				ArrayList<Ref> references = GR.getALLRef();  // the entire references where the key exists
				HashSet<String> hs = new HashSet<String>();	 // the names of pages where there is a key	
				
				for(Ref r: references)
					hs.add(r.getPage());
				
				for(String p_name:hs)
				{
					//System.out.println("The page name : "+p_name);
					Page p = Table.deserialize(p_name); 
					
					int i=0;
					while (i < p.getTuples().size()) 
					{
						Tuple current = p.getTuples().get(i);
		
						if (!current.getAttributes().get(y.getPrimaryPos()).equals(key)) 
							{
							i++;
							Comparable c = (Comparable)current.getAttributes().get(y.getPrimaryPos());
							if(c.compareTo(key) < 0)
								continue;
							
							break;
							}
						
						
						// loop over the current tuple 
						//System.out.println(current.getAttributes().size());
						for (int k = 0; k < current.getAttributes().size()-2; k++) 
						{
//							System.out.printf("k=%d, %s\n",k,colnames.get(k));
							if (htblColNameValue.containsKey(colnames.get(k))) 
							{
//								System.out.println(k+", before :"+current);
								
								if(indexed.get(k))
								{	
//									System.out.println("index changed on "+ colnames.get(k));
									TreeIndex t = y.getColNameBTreeIndex().get(colnames.get(k));
//									System.out.println("The tree before "+t.toString());
									Comparable old_value = (Comparable)current.getAttributes().get(k);
									Comparable new_value = (Comparable)htblColNameValue.get(colnames.get(k));
									t.delete(old_value, p_name);
									t.insert(new_value, new Ref(p_name));
//									System.out.println("The tree after "+t.toString());
									
								}
	
								current.getAttributes().setElementAt(htblColNameValue.get(colnames.get(k)), k);
								
//								System.out.println(k+", after :"+current);
								// update the trees
								
							
							}
							
						}
						Date date = new Date();
						current.getAttributes().setElementAt(date, current.getAttributes().size()-2);
		
						i++;
					}
					
					System.out.println("page after: \n"+p);
					p.serialize();
				}
				
			}
			// if the key is not indexed use the Binary search
			else
			{
			System.out.println("BINARY SEARCH USED");
			String[] searchResult = y.SearchInTable(strTableName, strClusteringKey).split("#");
			Page p = Table.deserialize(searchResult[0]);
			int i = Integer.parseInt(searchResult[1]);
			
			int j = y.getPages().indexOf(searchResult[0]);
			boolean flag = true;
			while (j < y.getPages().size() && flag) 
			{
				p = Table.deserialize(y.getPages().get(j));
				
				while (i < p.getTuples().size()&& flag) 
				{
					Tuple current = p.getTuples().get(i);
	
					if (!current.getAttributes().get(y.getPrimaryPos()).equals(key)) 
					{
						i++;
						flag = false;
						break;
					}
	
					for (int k = 0; k < current.getAttributes().size()-2; k++) 
					{
//						System.out.printf("k=%d, %s\n",k,colnames.get(k));
						if (htblColNameValue.containsKey(colnames.get(k))) {
							
//							System.out.println(k+", before :"+current);
							
							// update the trees
							if(indexed.get(k))
							{
//								System.out.println("index changed on "+ colnames.get(k));
								TreeIndex t = y.getColNameBTreeIndex().get(colnames.get(k));
//								System.out.println("The tree before "+t.toString());
								Comparable old_value = (Comparable)current.getAttributes().get(k);
								Comparable new_value = (Comparable)htblColNameValue.get(colnames.get(k));
								t.delete(old_value, searchResult[0]);
								t.insert(new_value, new Ref(searchResult[0]));
//								System.out.println("The tree after "+t.toString());

							}
							
							current.getAttributes().setElementAt(htblColNameValue.get(colnames.get(k)), k);
//							System.out.println(k+", after :"+current);
							
						
							
							
						}
						
					}
					Date date = new Date();
					current.getAttributes().setElementAt(date, current.getAttributes().size()-2);
					i++;
				}
				j++;
				i = 0;
//				System.out.println("page after: "+p);
				p.serialize();
			}
			}
		}
		catch(ClassNotFoundException e) {
			throw new DBAppException("Class not found Exception");
		}
		
		serialize(y);
		System.out.println("||||\t\tEnd Updating\t\t||||");
	}
	
	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException {
		System.out.println("||||\t\tStart Deleting\t\t||||");
		Table y = deserialize(strTableName);
		Vector meta = readFile("data/metadata.csv");
		Vector<String[]> metaOfTable = new Vector();
		for (Object o : meta) {
			String[] line = (String[]) o;
			if (line[0].equals(strTableName)) {
				metaOfTable.add(line);
			}
		}
		String clusteringKey = isThereACluster(htblColNameValue, strTableName);
		y.deleteInTable(htblColNameValue, metaOfTable ,clusteringKey);
		serialize(y);
		System.out.println("||||\t\tEnd Deleting\t\t||||");
	}
	public String isThereACluster(Hashtable<String, Object> htblColNameValue , String strTableName) throws DBAppException
	{
		Vector meta = readFile("data/metadata.csv");
		Vector<String[]> metaOfTable = new Vector();
		for (Object o : meta) {
			String[] line = (String[]) o;
			if (line[0].equals(strTableName)) {
				if(line[3].equals("True"))
				{
					Set<String> keyss = htblColNameValue.keySet();
					for(String key : keyss)
					{
						if(key.equals(line[1]))
						{
							return ""+htblColNameValue.get(key);
						}
					}
					
//					  String val = (String) htblColNameValue.get(line[1]);
//					  if (val!=null){
//					  	return ///?line[3];
//					  }
					 
				}
			}
		}
		return null;
	}
	public static void serialize(Table table) throws DBAppException {
		try {
			System.out.println("IO||||\t serialize:table:"+table.getTableName());
			FileOutputStream fileOut = new FileOutputStream("data/"+table.getTableName() + ".class");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(table);
			out.close();
			fileOut.close();
		}
		catch(IOException e) {
			e.printStackTrace();
			throw new DBAppException("IO Exception");
		}
	}

	public static Table deserialize(String tableName) throws DBAppException {
		try {
			System.out.println("IO||||\t deserialize:table:"+tableName);
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
		}
		catch (ClassNotFoundException e) {
			throw new DBAppException("Class Not Found Exception");
		}
	}

	public static Vector readFile(String path) throws DBAppException {
		try {
			System.out.println("IO||||\t readFile :"+path);
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
	
	
	public void createBTreeIndex(String strTableName,String strColName) throws DBAppException, IOException{
		BPTree bTree=null;
		Vector meta = readFile("data/metadata.csv");
		String colType="";
		int colPosition = -1;
		for (Object O : meta) {
			String[] curr = (String[]) O;
			if (curr[0].equals(strTableName)) {
				colPosition++;
				if (curr[1].equals(strColName)) {
					colType = curr[2];
					curr[4] = "True";
					break;
				}
			}
		}

		FileWriter csvWriter = new FileWriter("data/metadata.csv");
		for (Object O : meta) {
			String[] curr = (String[]) O;
			for (int j = 0; j < curr.length; j++) {
				csvWriter.append(curr[j]);
				csvWriter.append(",");
			}
			csvWriter.append("\n");
		}
		csvWriter.flush();
		csvWriter.close();

		switch(colType){
			case "java.lang.Integer":bTree=new BPTree<Integer>(nodeSize);break;
			case "java.lang.Double":bTree=new BPTree<Double>(nodeSize);break;
			case "java.util.Date":bTree=new BPTree<Date>(nodeSize);break;
			case "java.lang.Boolean":bTree=new BPTree<Boolean>(nodeSize);break;
			case "java.lang.String":bTree=new BPTree<String>(nodeSize);break;
			case "java.awt.Polygon":throw new DBAppException("A B+ Tree Index cannot be created on a column of type Polygon ! You can create an R-Tree index instead!");
			default :throw new DBAppException("I've never seen this colType in my life");
		}
		Table table =deserialize(strTableName);
		table.createBTreeIndex(strColName,bTree,colPosition);
		serialize(table);
	}

	

	public void createRTreeIndex(String strTableName,String strColName) throws DBAppException, IOException{
		RTree rTree=null;
		Vector meta = readFile("data/metadata.csv");
		String colType="";
		int colPosition = -1;
		for (Object O : meta) {
			String[] curr = (String[]) O;
			if (curr[0].equals(strTableName)) {
				colPosition++;
				if (curr[1].equals(strColName)) {
					colType = curr[2];
					curr[4] = "True";
					break;
				}
			}
		}

		FileWriter csvWriter = new FileWriter("data/metadata.csv");
		for (Object O : meta) {
			String[] curr = (String[]) O;
			for (int j = 0; j < curr.length; j++) {
				csvWriter.append(curr[j]);
				csvWriter.append(",");
			}
			csvWriter.append("\n");
		}
		csvWriter.flush();
		csvWriter.close();

		switch(colType){
			case "java.awt.Polygon":rTree=new RTree<Polygons>(nodeSize);break;
			default :throw new DBAppException("R-Tree index can be created only on columns of type Polygon !");
		}
		Table table =deserialize(strTableName);
		table.createRTreeIndex(strColName,rTree,colPosition);
		serialize(table);
	}


	
	public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
			 String[] strarrOperators)
			throws DBAppException {
		System.out.println("||||\t\tStart Selecting\t\t||||");
		String strTableName=arrSQLTerms[0]._strTableName;
		Table t=deserialize(strTableName);
		Vector meta = readFile("data/metadata.csv");
		Vector<String[]> metaOfTable = new Vector();
		for (Object o : meta) {
			String[] line = (String[]) o;
			if (line[0].equals(strTableName)) {
				metaOfTable.add(line);
			}
		}
		Iterator<Tuple> out=t.selectFromTable(arrSQLTerms,strarrOperators,metaOfTable);
		System.out.println("||||\t\tEnd Selecting\t\t||||");
		return out;
	}
	public void dropTable(String strTableName) throws DBAppException{
		try {
			Table tableToBeDeleted = deserialize(strTableName);
			tableToBeDeleted.drop();
			File tableFile = new File("data/"+strTableName+".class");
			System.out.println("/////||||\\\\\\\\\\\\\\\\\\deleting file "+strTableName);
			tableFile.delete();
			deleteFromMetadata(strTableName);
		}
		catch (DBAppException d) {
			d.printStackTrace();
		}
	}
	public static void deleteFromMetadata(String strTableName)throws DBAppException {
		try {
			Vector<String[]> meta = readFile("data/metadata.csv");
			Vector<String[]> result = new Vector<>();
			for (String[] curLine : meta) {
				if (!curLine[0].equals(strTableName))
					result.add(curLine);
			}
			FileWriter csvWriter = new FileWriter("data/metadata.csv");
			for (String[] curr : result) {
				for (int j = 0; j < curr.length; j++) {
					csvWriter.append(curr[j]);
					csvWriter.append(",");
				}
				csvWriter.append("\n");
			}
			csvWriter.flush();
			csvWriter.close();
			
		}
		catch(IOException e) {
			throw new DBAppException("IO Exception while modifying metadata to delete a table");
		}
	}
	public static void main(String[] args) throws DBAppException {
	/*	SQLTerm[] hai=new SQLTerm[2]; QUESTION
		for(int i=0;i<hai.length;i++) {
			SQLTerm x=new SQLTerm();
			hai[i]=x;
		}
		hai[0]._strTableName="a";
		System.out.println(hai[0]._strTableName); */

		
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
	}

	public void ignoreMe() throws DBAppException {
		Table y = deserialize("Student");
		TreeIndex x=y.getColNameBTreeIndex().get("id");
		if(x.search(2343432)!=null) {
			System.out.println("felsaleem");
		}else {
			System.out.println("we are doomed");
		}
	}

	
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