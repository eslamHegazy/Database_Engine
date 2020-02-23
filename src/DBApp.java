import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.io.*;
import java.util.*;
public class DBApp {
	static Vector tables=new Vector();
	public static Vector readFile(String path) throws IOException{
		String currentLine = "";
		FileReader fileReader= new FileReader(path);
		BufferedReader br = new BufferedReader(fileReader);
		Vector metadata=new Vector();
		while ((currentLine = br.readLine()) != null) {
			metadata.add(currentLine.split(","));
		}
		return metadata;
	}
	public static void createTable(String strTableName, String strClusteringKey,
			Hashtable<String, String> htblColNameType) throws Exception {
		Table table = new Table();
		table.setTableName(strTableName);
		table.setStrClusteringKey(strClusteringKey);
		try (FileWriter writer = new FileWriter(
				new File("metadata.csv"), true)) {
			

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
	public void insertIntoTable(String strTableName,Hashtable<String,Object>  htblColNameValue) throws Exception  {
		Table y=null;
		Object keyValue=null;
		for(Object x:tables) {
			 y=(Table)x;
			if(y.getTableName().equals(strTableName)) {
				break;
			}
		}
		if(y==null) {
			System.err.println("NoSuchTable");
			return;
		}
		Tuple newEntry=new Tuple();
		int i=0;
			Vector meta = readFile("metadata.csv");
			for(Object O: meta)
			{
				String[] curr = (String[]) O;
				
				if(curr[0].equals(strTableName))
				{
					String name=curr[1];
					String type=curr[2];
					if(!htblColNameValue.contains(name)) {
						System.err.println("col name invalid");
					}else {
						//String strColType=(String) colTypes.get(i++);
						Class colType = Class.forName( type );
						Class parameterType = htblColNameValue.get(name).getClass();
						if(!colType.equals(parameterType))
						{
							System.err.println("DATA types 8alat");
							return;
						}else {
							newEntry.addAttribute(htblColNameValue.get(name));
							if(Boolean.parseBoolean(curr[3])) {
								y.setPrimaryPos(i);
								keyValue=htblColNameValue.get(name);
								
							}
						}
					}
					i++;
				}
			}
			
		
		y.insertSorted(newEntry,keyValue); //TODO
		
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		/*String strTableName = "Student";
		Hashtable htblColNameType = new Hashtable();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.double");
		createTable(strTableName, "id", htblColNameType);*/
		Object x=new String("habdoo");
		String y="java.lang.String";
		String strColType=y;
		Class colType = Class.forName( strColType );
		Class parameterType = x.getClass();
		if(!colType.equals(parameterType))
		{
			System.err.println("DATA types 8alat");
		}else {
			System.out.println("LalO");
		}
	}
}
