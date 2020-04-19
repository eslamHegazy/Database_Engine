package kalabalaDB;

import java.awt.List;
import java.awt.Polygon;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Locale;
import java.util.Scanner;
import java.util.Vector;

import BPTree.BPTree;
import General.Ref;


public class DBAppTest {
	static void schema() throws Exception{
		String strTableName = "Schema";
		String strClusteringKey = "id";
		Hashtable h = new Hashtable<>();
		h.put("id", "java.lang.Integer");
		h.put("name", "java.lang.String");
		h.put("gpa", "java.lang.Double");
		h.put("birth", "java.util.Date");
		h.put("male", "java.lang.Boolean");
		h.put("shape", "java.awt.Polygon");
		DBApp d = new DBApp(); d.init(); 
		d.createTable(strTableName, strClusteringKey, h); 
	}
	static void indices()throws Exception{
		DBApp d = new DBApp(); d.init(); 
		String strTableName = "Schema";
		String strClusteringKey = "id";
		Hashtable h = new Hashtable<>();
//		d.createBTreeIndex(strTableName, "id");
		d.createBTreeIndex(strTableName, "name");
//		d.createBTreeIndex(strTableName, "gpa");
		d.createBTreeIndex(strTableName, "birth");
		d.createBTreeIndex(strTableName, "male");
		
//		d.createRTreeIndex(strTableName, "shape");
		
	}
	static void fill() throws Exception{
		DBApp d = new DBApp(); d.init();
		String strTableName="Schema";
		int n = 45;
		Hashtable h = new Hashtable<>();
		for (int i=0;i<n;i++) {
			h.put("id", new Integer( (int)(Math.random()*40) ));
			h.put("name", new String(randomAlphaNumeric(3)));
			h.put("gpa", new Double( 1.0*(int)(Math.random()*100)/100 ));
			h.put("birth", randomDate());
			h.put("male", (int)(Math.random()*2)==0 );
			h.put("shape", randomPolygon());
			d.insertIntoTable(strTableName, h);
		}
		
	}
	static void tryDel() throws Exception{
		DBApp d = new DBApp(); d.init();
		Hashtable h = new Hashtable<>();
//		Polygon del = Polygons.parsePolygon("(13,14),(1,7),(11,1),(14,4)");
//		Polygon del = Polygons.parsePolygon("(13,0),(13,12),(0,13)");
//		h.put("shape", del);
//		h.put("name", "Zaky Noor");
//		h.put("gpa", 1.5);
		h.put ("id",1118361111);
		h.put("shape", new Polygon());
//		h.put ("name","Arousiiii");
		d.deleteFromTable("Student", h);
	}
	
	static void tstovf()throws Exception{
		DBApp d = new DBApp(); d.init();
		Hashtable h = new Hashtable<>();
//		h.put("id", "java.lang.Integer");
//		h.put("t", "java.lang.Integer");
//		d.createTable("tb", "id", h);
//		d.createBTreeIndex("tb", "id");
//		
//		for(int i=0;i<7;i++) {
//			h.clear();
//			h.put("id", 1);
//			h.put("t",i);
//			d.insertIntoTable("tb", h);
//		}
		h.put("id", 1);
		h.put("t", 6);
		d.deleteFromTable("tb", h);
	}
	static Iterator<Tuple> s1() throws Exception{
		DBApp d = new DBApp(); d.init();
		String strTableName="Schema";
		SQLTerm[] arrSQLTerms = new SQLTerm[1];
		String[] strarrOperators = new String[0];
		
		arrSQLTerms[0] = new SQLTerm();
		SQLTerm s = arrSQLTerms[0];
		s._strTableName=strTableName;
		s._strColumnName="birth";
		s._strOperator = "<";
		s._objValue = DBApp.parseDate("2000-02-24");
//		s._objValue = DBApp.parseDate("2019-04-30");
		return d.selectFromTable(arrSQLTerms, strarrOperators);
	}
	static Iterator<Tuple> s2() throws Exception{
		DBApp d = new DBApp(); d.init();
		String strTableName="Schema";
		SQLTerm[] arrSQLTerms = new SQLTerm[1];
		String[] strarrOperators = new String[0];
		
		arrSQLTerms[0] = new SQLTerm();
		SQLTerm s = arrSQLTerms[0];
		s._strTableName=strTableName;
		s._strColumnName="name";
		s._strOperator = "<";
		s._objValue = "Esso";
//		s._objValue = DBApp.parseDate("2019-04-30");
		return d.selectFromTable(arrSQLTerms, strarrOperators);
	}
	
	static Iterator<Tuple> s3() throws Exception{
		DBApp d = new DBApp(); d.init();
		String strTableName="Schema";
		SQLTerm[] arrSQLTerms = new SQLTerm[2];
		String[] strarrOperators = new String[1];
		
		arrSQLTerms[0] = new SQLTerm();
		SQLTerm s = arrSQLTerms[0];
		s._strTableName=strTableName;
		s._strColumnName="male";
		s._strOperator = "=";
		s._objValue = true;
		
		arrSQLTerms[1] = new SQLTerm();
		s = arrSQLTerms[1];
		s._strTableName=strTableName;
		s._strColumnName="male";
		s._strOperator = "=";
		s._objValue = false;
		strarrOperators[0]="AND";
//		s._objValue = DBApp.parseDate("2019-04-30");
		return d.selectFromTable(arrSQLTerms, strarrOperators);
	}
	
	static Iterator<Tuple> s4() throws Exception{
		DBApp d = new DBApp(); d.init();
		String strTableName="Schema";
		SQLTerm[] arrSQLTerms = new SQLTerm[1];
		String[] strarrOperators = new String[0];
		
		arrSQLTerms[0] = new SQLTerm();
		SQLTerm s = arrSQLTerms[0];
		s._strTableName=strTableName;
		s._strColumnName="male";
		s._strOperator = "=";
		s._objValue = true;
		
//		s._objValue = DBApp.parseDate("2019-04-30");
		return d.selectFromTable(arrSQLTerms, strarrOperators);
	}
	static Iterator<Tuple> s5() throws Exception{
		DBApp d = new DBApp(); d.init();
		String strTableName="Schema";
		SQLTerm[] arrSQLTerms = new SQLTerm[1];
		String[] strarrOperators = new String[0];
		
		arrSQLTerms[0] = new SQLTerm();
		SQLTerm s = arrSQLTerms[0];
		s._strTableName=strTableName;
		s._strColumnName="male";
		s._strOperator = "<";
		s._objValue = true;
		
//		s._objValue = DBApp.parseDate("2019-04-30");
		return d.selectFromTable(arrSQLTerms, strarrOperators);
	}
	
	static Iterator<Tuple> s6() throws Exception{
		DBApp d = new DBApp(); d.init();
		String strTableName="Schema";
		SQLTerm[] arrSQLTerms = new SQLTerm[1];
		String[] strarrOperators = new String[0];
		
		arrSQLTerms[0] = new SQLTerm();
		SQLTerm s = arrSQLTerms[0];
		s._strTableName=strTableName;
		s._strColumnName="male";
		s._strOperator = "=";
		s._objValue = false;
		
//		s._objValue = DBApp.parseDate("2019-04-30");
		return d.selectFromTable(arrSQLTerms, strarrOperators);
	}
	
	
	static Iterator<Tuple> s7() throws Exception{
		
		DBApp d = new DBApp(); d.init();
		String strTableName="Schema";
		SQLTerm[] arrSQLTerms = new SQLTerm[3];
		String[] strarrOperators = new String[2];
		
		arrSQLTerms[0] = new SQLTerm();
		SQLTerm s = arrSQLTerms[0];
		s._strTableName=strTableName;
		s._strColumnName="id";
		s._strOperator = ">=";
		s._objValue = 5;
		
		arrSQLTerms[1] = new SQLTerm();
		s = arrSQLTerms[1];
		s._strTableName=strTableName;
		s._strColumnName="birth";
		s._strOperator = ">";
		s._objValue = DBApp.parseDate("2015-01-01");
		strarrOperators[0]="OR";

		arrSQLTerms[2] = new SQLTerm();
		s = arrSQLTerms[2];
		s._strTableName=strTableName;
		s._strColumnName="id";
		s._strOperator = "<";
		s._objValue = 20;
		strarrOperators[1]="XOR";
		
		return d.selectFromTable(arrSQLTerms, strarrOperators);
		
	}
	

	
	static void select() throws Exception{
//		showit(s1());
//		showit(s2());
//		showit(s3());
//		showit(s4());
//		showit(s5());
//		showit(s6());
		showit(s7());
//		Table k = new Table();
//		System.out.println(k.differenceSets((ArrayList<Tuple>)s4(), (ArrayList<Tuple>)s5()));
	}
	
	
	static void showit(Iterator<Tuple> i) {
		boolean fnd = false;
		int cnt = 0;
		while (i.hasNext()) {
			fnd = true;
			Tuple t = i.next(); int last = t.getAttributes().size()-1;
			System.out.println(t);//.getAttributes().get(last)+"\t");// .getAttributes().get(2));
			cnt++;
		}
		if(!fnd) System.out.println("<<<>>>><<<>>> SELECT RETRUNED EMPTY <<<<>>>><<<<>>>><<<>>>");
		else {System.out.printf("<<<<<<<<<< FOUND %d Matching Tuples\n",cnt);}
	}
	
	public static void main(String[] args)throws Exception {
//		clear();
		
//		schema();
//		indices();
		
		long st = System.nanoTime();
//		fill();

//		DBApp  d= new DBApp(); d.init(); 
//		Hashtable h = new Hashtable<>();
//		h.put("id", new Integer( (int)(Math.random()*40) ));
//		h.put("name","21L");
//		h.put("gpa", new Double( 1.0*(int)(Math.random()*100)/100 ));
//		h.put("birth", randomDate());
//		h.put("male", (int)(Math.random()*2)==0 );
//		h.put("shape", randomPolygon());
//		d.insertIntoTable("Schema", h); 
//		
		select();
//		tryDel();
		long end = System.nanoTime(); 
//		showAt0s();
		
		
		System.err.printf("Taken %.3f sec\n",(end-st)/1e9);

//		Boolean b1 = true;
//		Boolean b2 = false;
//		Boolean b3 = new Boolean(true);
//		Boolean b4 = new Boolean(false);
//		Boolean b5 = null;
//		System.out.println(b5.compareTo(b1));
//		Date d0 = DBApp.parseDate("2000-02-01");
//		Date d1 = DBApp.parseDate("2000-02-24");
//		Date d2 = DBApp.parseDate("2000-02-28");
//		Date d3 = DBApp.parseDate("2000-05-10");
//		Date d4 = DBApp.parseDate("2010-05-10");
//		System.out.println(d4.compareTo(d0));
		
//		faUpTs();		
//		ad();
		
//		showAt0s();
		
//		deleteTest();
	}

	static void deleteTest() throws DBAppException, IOException{
		DBApp d = new DBApp(); d.init(); deleteTest(d);
	}
	@SuppressWarnings("unused")
	private static void selectWRabenaYstorha(DBApp dbApp) throws DBAppException {
		SQLTerm[] arrSQLTerms = new SQLTerm[2];
		String[] strarrOperators = new String[1];
		SQLTerm s = new SQLTerm();
		s._strTableName= "Student";
		s._strColumnName = "gpa";
		s._strOperator = ">";
		s._objValue =0.95;
		arrSQLTerms[1]=s;
		arrSQLTerms[0]= new SQLTerm();
		arrSQLTerms[0]._strTableName= "Student";
		arrSQLTerms[0]._strColumnName = "name";
		arrSQLTerms[0]._strOperator = "=";
		arrSQLTerms[0]._objValue ="Ahmed Noor" ;
		strarrOperators[0]="or";
		//long startTime = System.nanoTime();
		Iterator<Tuple> res = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
		//long endTime = System.nanoTime();

		// get difference of two nanoTime values
		//long timeElapsed = endTime - startTime;

		//System.out.println("Execution time in nanoseconds  : " + timeElapsed);

		//System.out.println("Execution time in milliseconds : " + 
			//					timeElapsed / 1000000);
		System.out.println();
		while(res.hasNext()) {
			System.out.println(res.next());
		}
	}

	static void deleteTest(DBApp dbApp) throws DBAppException,IOException
	{
		Hashtable htblColNameValue = new Hashtable();
		//htblColNameValue.put("id" , 1111111111);
		//htblColNameValue.put("name", "Arousiiii");
		htblColNameValue.put("gpa" , 1.5);
		Polygon p1 = new Polygon();
		p1.addPoint(1, 9);
		p1.addPoint(8, 0);
		//htblColNameValue.put("shape", p1);
		dbApp.deleteFromTable("Student", htblColNameValue);
	}
	static void ad ()throws Exception{
		DBApp d = new DBApp(); d.init();
//		2343432
		String strTableName="Student";
		Hashtable htblColNameValue = new Hashtable<>();
		for (int i=0;i<2;i++) {
		htblColNameValue.put("id", 2343432+(int)(Math.random()*1000+1));
		htblColNameValue.put("gpa", 1.0*((int)(Math.random()*100))/100);
		htblColNameValue.put("shape", randomPolygon());
		htblColNameValue.put("name", randomAlphaNumeric(6));
		d.insertIntoTable(strTableName, htblColNameValue);}
	}
	static void faUpTs() throws Exception{ DBApp d = new DBApp(); d.init(); faUpTs(d);}
	static void faUpTs(DBApp dbApp) throws Exception{
//		DBApp d = new DBApp();
//		d.init();

		String strTableName = "Student";
		
		
		Hashtable htblColNameType = new Hashtable();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.Double");
		htblColNameType.put("shape", "java.awt.Polygon");
		
		dbApp.createTable( strTableName, "id", htblColNameType );
		
		dbApp.createBTreeIndex( strTableName, "gpa" );
		dbApp.createBTreeIndex( strTableName, "id" );		
		dbApp.createRTreeIndex( strTableName, "shape" );
		dbApp.createBTreeIndex( strTableName, "name" );

		Hashtable htblColNameValue = new Hashtable();
		
		
		htblColNameValue.put("id", new Integer( 2343432 ));
		htblColNameValue.put("name", new String("Ahmed Noor" ) );
		htblColNameValue.put("gpa", new Double( 0.95 ) );
		Polygon p1 = new Polygon();
		p1.addPoint(1, 9);
		p1.addPoint(8, 0);
		htblColNameValue.put("shape", p1); //check Polygon work correctly
		dbApp.insertIntoTable( strTableName , htblColNameValue );
		
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 2343432 ));
		htblColNameValue.put("name", new String("Ahmed Noor" ) );
		htblColNameValue.put("gpa", new Double( 0.956 ) );
		htblColNameValue.put("shape", new Polygon()); //check Polygon work correctly
		dbApp.insertIntoTable( strTableName , htblColNameValue );
		
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 1111111111 ));
		htblColNameValue.put("name", new String("Arousiiii" ) );
		htblColNameValue.put("gpa", new Double( 1.5 ) );
		htblColNameValue.put("shape", new Polygon()); //check Polygon work correctly
		dbApp.insertIntoTable( strTableName , htblColNameValue );
		
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer(1));
		htblColNameValue.put("name", new String("Ahmed Noor" ) );
		htblColNameValue.put("gpa", new Double( 0.95 ) );
		htblColNameValue.put("shape", randomPolygon() );
		dbApp.insertIntoTable( strTableName , htblColNameValue );
		
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 5674567 ));
		htblColNameValue.put("name", new String("Dalia Noor" ) );
		htblColNameValue.put("gpa", new Double( 1.25 ) );
		htblColNameValue.put("shape",randomPolygon() );
		dbApp.insertIntoTable( strTableName , htblColNameValue);

		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 23498 ));
		htblColNameValue.put("name", new String("John Noor" ) );
		htblColNameValue.put("gpa", new Double( 1.5 ) );
		htblColNameValue.put("shape", randomPolygon() );
		dbApp.insertIntoTable( strTableName , htblColNameValue );
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 78452 ));
		htblColNameValue.put("name", new String("Zaky Noor" ) );
		htblColNameValue.put("gpa", new Double( 0.88 ) );
		htblColNameValue.put("shape", randomPolygon( ) );
		dbApp.insertIntoTable( strTableName , htblColNameValue );
		
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 23498 ));
		htblColNameValue.put("name", new String("John Noor" ) );
		htblColNameValue.put("gpa", new Double( 1.5 ) );
		htblColNameValue.put("shape", randomPolygon() );
		dbApp.insertIntoTable( strTableName , htblColNameValue );
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 23498 ));
		htblColNameValue.put("name", new String("John Noor" ) );
		htblColNameValue.put("gpa", new Double( 1.5 ) );
		htblColNameValue.put("shape", randomPolygon() );
		dbApp.insertIntoTable( strTableName , htblColNameValue );
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 4 ));
		htblColNameValue.put("name", new String("John Noor" ) );
		htblColNameValue.put("gpa", new Double( 1.5 ) );
		htblColNameValue.put("shape", randomPolygon() );
		dbApp.insertIntoTable( strTableName , htblColNameValue );
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 3 ));
		htblColNameValue.put("name", new String("Johnnnn Noor" ) );
		htblColNameValue.put("gpa", new Double( 1.5 ) );
		htblColNameValue.put("shape", randomPolygon() );
		dbApp.insertIntoTable( strTableName , htblColNameValue );
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 12 ));
		htblColNameValue.put("name", new String("Johnasd Noor" ) );
		htblColNameValue.put("gpa", new Double( 1.5 ) );
		htblColNameValue.put("shape", randomPolygon() );
		dbApp.insertIntoTable( strTableName , htblColNameValue );
		
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 1111111111 ));
		htblColNameValue.put("name", new String("Arousiiii" ) );
		htblColNameValue.put("gpa", new Double( 1.5 ) );
		htblColNameValue.put("shape", randomPolygon() );
		dbApp.insertIntoTable( strTableName , htblColNameValue );
		/*htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 23 ));
		htblColNameValue.put("name", new String("Arousiiii" ) );
		htblColNameValue.put("gpa", new Double( 1.5 ) );
		htblColNameValue.put("shape", randomPolygon() );
		dbApp.insertIntoTable( strTableName , htblColNameValue );
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 280 ));
		htblColNameValue.put("name", new String("Arousiiii" ) );
		htblColNameValue.put("gpa", new Double( 1.5 ) );
		htblColNameValue.put("shape", randomPolygon() );
		dbApp.insertIntoTable( strTableName , htblColNameValue );
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 23 ));
		htblColNameValue.put("name", new String("Arousiiii" ) );
		htblColNameValue.put("gpa", new Double( 1.5 ) );
		htblColNameValue.put("shape", randomPolygon() );
		dbApp.insertIntoTable( strTableName , htblColNameValue );
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 23 ));
		htblColNameValue.put("name", new String("Arousiiii" ) );
		htblColNameValue.put("gpa", new Double( 1.5 ) );
		htblColNameValue.put("shape", randomPolygon() );
		dbApp.insertIntoTable( strTableName , htblColNameValue );
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 23 ));
		htblColNameValue.put("name", new String("Arousiiii" ) );
		htblColNameValue.put("gpa", new Double( 1.5 ) );
		htblColNameValue.put("shape", randomPolygon() );
		dbApp.insertIntoTable( strTableName , htblColNameValue );
		//for (int i=0;i<1500;i++) {
		/*for (int i=0;i<10;i++) {*/
//		for (int i=0;i<100;i++) {
//			//System.out.println(i);
//			htblColNameValue.clear( );
////			htblColNameValue.put("id", new Integer( i ));
//			htblColNameValue.put("id", 1+(int)(Math.random()*200));
//			htblColNameValue.put("name", new String(randomAlphaNumeric(4)+"Noor" ) );
//			double gpa = 1.0*((int)(1+Math.random()*1000))/1000;
//			htblColNameValue.put("gpa", new Double( gpa ) );
//			htblColNameValue.put("shape", randomPolygon()); //Polygons.parsePolygons("(0,0),(7,6)" ) );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
//		}
		
		for(int i = 0 ; i < 5 ; i++) {
			htblColNameValue.clear( );
			htblColNameValue.put("id", new Integer( i ));
			htblColNameValue.put("name", new String(randomAlphaNumeric(4)+" RAND" ) );
			htblColNameValue.put("gpa", new Double( 1.5 ) );
			htblColNameValue.put("shape", randomPolygon() );
			dbApp.insertIntoTable( strTableName , htblColNameValue );
		}
		
		
	
	}
	static void tsSel1() throws DBAppException{
		DBApp d = new DBApp();
		d.init();
		SQLTerm[] arrSQLTerms = new SQLTerm[2];
		String[] strarrOperators = new String[1];
		SQLTerm s = new SQLTerm();
		s._strTableName= "Student";
		s._strColumnName = "id";
		s._strOperator = "=";
		s._objValue =9;
		arrSQLTerms[0]=s;
		arrSQLTerms[1]= new SQLTerm();
		arrSQLTerms[1]._strTableName= "Student";
		arrSQLTerms[1]._strColumnName = "name";
		arrSQLTerms[1]._strOperator = "=";
		arrSQLTerms[1]._objValue = "Ahmed Noor";
		strarrOperators[0]="XoR";
		Iterator<Tuple> res = d.selectFromTable(arrSQLTerms, strarrOperators);
		System.out.println();
		while(res.hasNext()) {
			System.out.println(res.next());
		}
	}

	static void tst10_h0() throws DBAppException,IOException{
		DBApp dbApp = new DBApp();
		dbApp.init();
		Hashtable htblColNameType = new Hashtable<>();
		String strTableName="t";
		htblColNameType.put("p", "java.awt.Polygon");
		htblColNameType.put("d", "java.lang.Integer");
		
		dbApp.createTable( "t", "p", htblColNameType);
				
//		dbApp.createRTreeIndex( strTableName, "p" );

		Hashtable htblColNameValue = new Hashtable();
		Polygon p1;
		
		htblColNameValue.clear();
		p1 = new Polygon(); 
		p1.addPoint(0, 0); p1.addPoint(0, 3); p1.addPoint(3, 0); p1.addPoint(3, 3);
		htblColNameValue.put("p", p1);
		htblColNameValue.put("d", (int)(Math.random()*11));
		dbApp.insertIntoTable(strTableName, htblColNameValue);
		
		htblColNameValue.clear();
		p1 = new Polygon(); 
		p1.addPoint(6, 6); p1.addPoint(6, 9); p1.addPoint(9, 6); p1.addPoint(9, 9);
		htblColNameValue.put("p", p1);
		htblColNameValue.put("d", (int)(Math.random()*11));
		dbApp.insertIntoTable(strTableName, htblColNameValue);
		
		htblColNameValue.clear();
		p1 = new Polygon(); 
		p1.addPoint(1, 5); p1.addPoint(2, -7); p1.addPoint(31, 4); p1.addPoint(9, 8);
		htblColNameValue.put("p", p1);
		htblColNameValue.put("d", (int)(Math.random()*11));
		dbApp.insertIntoTable(strTableName, htblColNameValue);
		
		htblColNameValue.clear();
		p1 = new Polygon(); 
		p1.addPoint(0, 0); p1.addPoint(6, 0); p1.addPoint(0, 3);
		htblColNameValue.put("p", p1);
		htblColNameValue.put("d", (int)(Math.random()*11));
		dbApp.insertIntoTable(strTableName, htblColNameValue);
		
		htblColNameValue.clear();
		p1 = new Polygon(); 
		p1.addPoint(0, 0); p1.addPoint(3, 0); p1.addPoint(0, 3);
		htblColNameValue.put("p", p1);
		htblColNameValue.put("d", (int)(Math.random()*11));
		dbApp.insertIntoTable(strTableName, htblColNameValue);
		
	}

	static void tst10_h1() throws Exception{
		DBApp d = new DBApp(); d.init();
		Polygon pk = new Polygon();
//		pk.addPoint(0, 0); pk.addPoint(0, 3); pk.addPoint(3, 0); pk.addPoint(3, 3);
//		pk.addPoint(0, 0); pk.addPoint(3, 0); pk.addPoint(0, 3);
//		pk.addPoint(6, 6); pk.addPoint(6, 9); pk.addPoint(9, 6); pk.addPoint(9, 9);
		pk = Polygons.parsePolygon("(1,5),(2,-7),(31,4),(9,8)");
		Hashtable htblColNameValue = new Hashtable<>();
		htblColNameValue.put("d", 192);
		d.updateTable("t", Polygons.clstrString(pk), htblColNameValue);
	}
	static void tst10_h2() throws Exception{
		DBApp d = new DBApp(); d.init();
		d.createRTreeIndex("t", "p");
	}
	static void tst10_h3() throws Exception{
		DBApp d = new DBApp(); d.init();
		Polygon pk = new Polygon();
		pk.addPoint(0, 0); pk.addPoint(3, 0); pk.addPoint(0, 3);
		Hashtable htblColNameValue = new Hashtable<>();
		htblColNameValue.put("p", pk);
		d.deleteFromTable("t", htblColNameValue);
	}
	static void tst10_h4() throws Exception{
		DBApp d= new DBApp(); d.init();
		String strTableName = "t";
		Hashtable htblColNameValue = new Hashtable();
		Polygon[] randPoly = new Polygon[7];
		for (int i=0;i<7;i++) {
			randPoly[i]=randomPolygon();
		}
		for(int i=0;i<30;i++) {
			htblColNameValue.clear();		
			htblColNameValue.put("p", randPoly[(int)(Math.random()*7)]);
			htblColNameValue.put("d", (int)(Math.random()*11));
			d.insertIntoTable(strTableName, htblColNameValue);
		}
		
		
	}
	
	
	static void tst9() throws DBAppException, IOException{
		String strTableName= "Bol";
		String strClusteringKey = "A";
		Hashtable htblColNameType = new Hashtable<>();
		htblColNameType.put("A", "java.lang.Boolean");
		htblColNameType.put("B", "java.lang.String");
		DBApp dbApp = new DBApp();
		dbApp.init();
//		dbApp.printAllPagesInAllTables("9A");
		dbApp.createTable(strTableName, strClusteringKey, htblColNameType);
		for (int i=0;i<50;i++) {
			boolean A= (int)(Math.random()*2)==0;
			String B = randomAlphaNumeric(7);
			Hashtable h = new Hashtable<>();
			h.put("A", A);
			h.put("B", B);
			dbApp.insertIntoTable(strTableName, h);
		}
		dbApp.printAllPagesInAllTables("9dn");

	}
	static void tsta() throws DBAppException, IOException{
		clear();
		
		
		String strTableName= "A";
		String strClusteringKey = "1";
		Hashtable htblColNameType = new Hashtable<>();
		htblColNameType.put("1", "java.lang.Integer");
		htblColNameType.put("2", "java.lang.String");
		DBApp dbApp = new DBApp();
		dbApp.init();
//		dbApp.printAllPagesInAllTables("AA");
		dbApp.createTable(strTableName, strClusteringKey, htblColNameType);
		for (int i=0;i<15;i++) {
			Hashtable htblColNameValue = new Hashtable<>();
			htblColNameValue.put("1", (int)(Math.random()*16));
			htblColNameValue.put("2", randomAlphaNumeric(4));
			dbApp.insertIntoTable(strTableName, htblColNameValue);
		}
		dbApp.printAllPagesInAllTables("AZ");
	}
	static void tstb() throws DBAppException, IOException{
		
		String strTableName= "B";
		String strClusteringKey = "1";
		Hashtable htblColNameType = new Hashtable<>();
		htblColNameType.put("1", "java.lang.Integer");
		htblColNameType.put("2", "java.lang.Double");
		DBApp dbApp = new DBApp();
		dbApp.init();
//		dbApp.printAllPagesInAllTables("BA");
		dbApp.createTable(strTableName, strClusteringKey, htblColNameType);
		for (int i=0;i<15;i++) {
			Hashtable htblColNameValue = new Hashtable<>();
			htblColNameValue.put("1", (int)(Math.random())*16);
			htblColNameValue.put("2", 20*Math.random());
			dbApp.insertIntoTable(strTableName, htblColNameValue);
		}
		dbApp.printAllPagesInAllTables("BZ");
	}
	

	static void tst8() throws  DBAppException, IOException{
		DBApp d = new DBApp();
		d.init();
		d.printAllPagesInAllTables("tst8-0");
		String strTableName = "booleanTest";
		String strClusteringKey = "C1";
		Hashtable htblColNameType = new Hashtable<>();
		htblColNameType.put("C1", "java.lang.Boolean");
		htblColNameType.put("C2", "java.util.Date");
		htblColNameType.put("C3", "java.lang.String");
		d.createTable(strTableName, strClusteringKey, htblColNameType);
		
		for (int i=0;i<19;i++) {
			boolean C1 = ((int)(Math.random()*10)%2==0);
			Date C2 = randomDate();
			String C3 = randomAlphaNumeric(6);
			Hashtable htblColNameValue= new Hashtable<>();
			htblColNameValue.put("C1", C1);
			htblColNameValue.put("C2", C2);
			htblColNameValue.put("C3", C3);
			d.insertIntoTable(strTableName, htblColNameValue);
		}
		d.printAllPagesInAllTables("tst8-1");
	}
	
	
	static void tst7() throws  DBAppException, IOException{
		DBApp d = new DBApp();
		d.init();
//		d.printAllPagesInAllTables("tst7-0");
		String strTableName = "dateTest";
		String strClusteringKey = "C1";
		Hashtable htblColNameType = new Hashtable<>();
		htblColNameType.put("C1", "java.util.Date");
		htblColNameType.put("C2", "java.util.Date");
		htblColNameType.put("C3", "java.lang.String");
		d.createTable(strTableName, strClusteringKey, htblColNameType);
		for (int i=0;i<19;i++) {
			Date C1 = randomDate();
			Date C2 = randomDate();
			String C3 = randomAlphaNumeric(6);
			Hashtable htblColNameValue= new Hashtable<>();
			htblColNameValue.put("C1", C1);
			htblColNameValue.put("C2", C2);
			htblColNameValue.put("C3", C3);
			d.insertIntoTable(strTableName, htblColNameValue);
		}
		d.printAllPagesInAllTables("tst7-1");
	}
	
	static Date randomDate() throws DBAppException{
		
		String res = "";
		int year = 1995+(int)(Math.random()*25);
//		int month = 1+(int)(Math.random()*12);
		int month = 1+(int)(Math.random()*4);
//		int day = (int)(Math.random()*31);
		int day = 5+(int)(Math.random()*10);
		if (month==2) {
			day = Math.max(day, 28);
		}
		else if (month == 4 || month == 6 || month == 9 || month == 11) {
			day = Math.max(day, 30);
		}
		res = String.format("%04d-%02d-%02d", year,month,day);
		System.out.println(res);
		Date d = DBApp.parseDate(res);
		System.out.println(d.toString());
		return d;
		
		
	}
	
	static void tst6() throws DBAppException, IOException{
		DBApp d = new DBApp();
		d.init();
		d.printAllPagesInAllTables("tst6-0");
		Scanner sc = new Scanner(System.in);
		String s = "";
		int n =9;
		for (int i=1;i<=n;i++) {
			Hashtable h = new Hashtable<>();
			String strClusteringKey = "";
			while ( !(s=sc.next()).toLowerCase().equals("u") ) {
				String coln = s.toUpperCase();
				String value = sc.next();
				
				if (coln.equals("C1")) {
					strClusteringKey=value;
				}
				else {
					if (coln.equals("C2")) {
						String C2 = value;
						h.put("C2", C2);
//						out.println(C2);
					}
					else {
						double C3 = Double.parseDouble(value);
						h.put("C3", C3);
//						out.println("\t"+C3+"\t");
					}
//					h.put(coln, value);
				}
			}
			d.updateTable("T1", strClusteringKey, h);
			d.printAllPagesInAllTables("tst6-"+i);
		}
		d.printAllPagesInAllTables("tst6-"+(n+1));
	}
	static void tst5() throws Exception{
		showCurrentState("tst5-st");
		DBApp d = new DBApp();
		d.init();
		Scanner sc = new Scanner(System.in);
		for (int i=0;i<7;i++) {
			String s ="";
			Hashtable h = new Hashtable<>();
			while(!(s=sc.next()).equals("d")&&!s.equals("D")) {
				String col = s.toUpperCase();
				String val = sc.next();
//				System.out.println(col+" "+val);
				if (col.equals("A")) {
					boolean C1 = Boolean.parseBoolean(val);
					h.put("A", C1);
				}
				else if (col.equals("B")) {
					h.put("B", val);
				}
//				else {
//					h.put("C3", Double.parseDouble(val));
//				}
				
			}
			System.out.println("About to delete");
			d.deleteFromTable("Bol", h);
			d.printAllPagesInAllTables("tst5-"+i);
			
		}
//		Hashtable h = new Hashtable<>();
//		h.put("C1",7);
//		System.out.println("About to delete");
//		d.deleteFromTable("T1", h);
//		showCurrentState("tst5-");
		showCurrentState("tst5-en");
	}
	
	static void showCurrentState() throws DBAppException {
		showCurrentState("neww");
	}
	static void showCurrentState(String name) throws DBAppException{
		DBApp D = new DBApp();
		D.init();
		D.printAllPagesInAllTables(name);
	}
	static void tst44() throws Exception{
		DBApp d = new DBApp();
		d.init();
		d.printAllPagesInAllTables("tst44_st");
		System.out.println();
		Hashtable t = new Hashtable<>();
		t.put("C3", 321.0122121);
		int key = 3;
		d.updateTable("T1", ""+key, t);
//		d.printAllPagesInAllTables("tst44_en");
	}
	
	static void tst3() throws Exception{
		DBApp d = new DBApp();
		d.init();
		tst1(d);
		d.printAllPagesInAllTables("tst1done");
		d=new DBApp();
//		d.printAllPagesInAllTables("new DBApp");
		
	}
	static void tst4() throws DBAppException, IOException{
		clear();
		DBApp d = new DBApp();
		d.init();
		Hashtable h = new Hashtable<>();
		h.put("C1", "java.lang.Integer");
		h.put("C2", "java.lang.String");
		h.put("C3", "java.lang.Double");
		d.createTable("T1", "C1", h);
		h = new Hashtable<>();
		for (int i=0;i<12;i++) {
			h = new Hashtable<>();
			int C1 = (int)(8*Math.random());
			String C2 = randomAlphaNumeric(1+(int)(Math.random()*7));
			double C3 = Math.random()*20;
			h.put("C1", C1);
			h.put("C2", C2);
			h.put("C3", C3);
			d.insertIntoTable("T1", h);
		}
		d.printAllPagesInAllTables("tst4-1");
		System.out.println();
//		Scanner sc = new Scanner(System.in);
		PrintWriter out = new PrintWriter("tst4updates.txt");
		for (int i=0;i<18;i++) {
//			int C1 = sc.nextInt();
//			String nxt = sc.next();
			int C1 = (int)(Math.random()*8);
			h = new Hashtable<>();
			out.print(i+":\t"+C1+"\t");
			String C2 = randomAlphaNumeric(6);
			h.put("C2", C2);
			double C3 = Math.random()*999;
			h.put("C3", C3);
//			if (nxt.equals("C2")) {
//				String C2 = sc.next();
//				h.put("C2", C2);
//				out.println(C2);
//			}
//			else {
//				double C3 = sc.nextDouble();
//				h.put("C3", C3);
//				out.println("\t"+C3+"\t");
//			}
			try {
				d.updateTable("T1", ""+C1, h);
			}
			catch(DBAppException e) {
				e.printStackTrace();
				i--;
				continue;
				
			}
				
			d.printAllPagesInAllTables("tst4-2-"+i);
		}
	}
	static void clear() {
//		File metadata = new File("data/metadata.csv");
//		metadata.delete();
		File data = new File("data");
		String[] files = data.list();
		for (String p: files ) {
			File fileToDelete = new File("data/"+p);
//			pageToDelete.delete();
			delete(fileToDelete);
		}
	}
	static void delete(File file) {
//		File file = new File(path);
		if (file.isDirectory()) {
			File[] files = file.listFiles();
			for (int i=0;i<files.length;i++) {
				delete(files[i]);
			}
		}
		file.delete();
	}
	private static final String ALPHA_NUMERIC_STRING = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

	public static String randomAlphaNumeric(int count) {
		StringBuilder builder = new StringBuilder();
		while (count-- != 0) {
			int character = (int) (Math.random() * ALPHA_NUMERIC_STRING.length());
			builder.append(ALPHA_NUMERIC_STRING.charAt(character));
		}
		return builder.toString();
	}

	static void tst1() throws Exception{
		DBApp dbApp = new DBApp();
		tst1(150, dbApp);
	}
	static void tst1(DBApp dbApp) throws Exception{
		tst1(150,dbApp);
	}
	static void tst1(int n) throws Exception{
		DBApp dbApp = new DBApp();
		tst1(n, dbApp);
	}
	static void tst1(int n,DBApp dbApp) throws Exception{//TODO:DBAppException
		//Testing Many insertions with many duplicate keys in a random order
//		100 insertions		1 sec
//		500 insertions 		17 sec
//		1000 insertions		54 sec
//		2000 insertions		190 sec
//		5000 insertions		1030 sec
//		DBApp dbApp = new DBApp();
		dbApp.init();
		Hashtable<String, String> htblColNameType = new Hashtable<>();
		String strTableName = "Esso Table";
		htblColNameType.put("the key", "java.lang.Integer");
		htblColNameType.put("garbage", "java.lang.Integer");
		htblColNameType.put("Txt", "java.lang.String");
		htblColNameType.put("float qnt", "java.lang.Double");
		dbApp.createTable(strTableName, "the key", htblColNameType);
		File file = new File("Data/tst1_Insertions.txt");
		PrintWriter out = new PrintWriter(file);
		long star = System.nanoTime();
		for (int i=0;i<n;i++) {
			
			int key = 12345+(int)(Math.random()*55);
			double fl = Math.random()*1000000;
			int grbg = (int)(Math.random()*927112);
			String st = randomAlphaNumeric((int)(Math.random()*15));
			Hashtable insrt = new Hashtable<>();
			insrt.put("the key", new Integer(key));
			insrt.put("garbage", new Integer(grbg));
			insrt.put("Txt", st);
			insrt.put("float qnt",new Double(fl));
			String thi = String.format("i=%d:  %d %d %s %f\n\n",i,key,grbg,st,fl);
			out.print(thi);
			dbApp.insertIntoTable(strTableName, insrt);
			System.out.println(i);
		}
		long end = System.nanoTime();
		long dur =( end-star)/(long)1e9;
		System.out.println(n+" insertions, Elapsed Time= "+dur+" seconds");
		out.close();
		dbApp.printAllPagesInAllTables();
	}
	static void tst2() throws Exception{
		DBApp dbApp = new DBApp();
		String strTableName = "Another Table";
		Hashtable<String, String> htblColNameType = new Hashtable<>();
		String[] labels= {"Esso","Body","Abdallah","Ahmed","Arousi","Fathy","Hela","Bela","nice","not nice","7elwa","balwa","thanks"};
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("poly", "java.awt.Polygon");
		htblColNameType.put("label", "java.lang.String");
		dbApp.createTable(strTableName, "id", htblColNameType);
		File file = new File("Data/tst2_Insertions.txt");
		PrintWriter out = new PrintWriter(file);
		long star = System.nanoTime();
		for (int i=0;i<450;i++) {
			
			int id = 100+(int)(Math.random()*25);
			Polygon p = randomPolygon();
			String st = labels[(int)(Math.random()*labels.length)];
			Hashtable insrt = new Hashtable<>();
			insrt.put("id", new Integer(id));
			insrt.put("poly", p);
			insrt.put("label", st);
			String thi = String.format("i=%d:  %d %s %s\n",i,id,p,st);
			out.print(thi);
			dbApp.insertIntoTable(strTableName, insrt);
			System.out.println(i);
		}
		long end = System.nanoTime();
		long dur =( end-star)/(long)1e9;
		out.println();
		System.out.println("Elapsed Time= "+dur+" seconds");
		out.println("Elapsed Time= "+dur+" seconds");
		out.flush();
//		dbApp.printAllPagesInAllTables("tst2_dataAfter");
		
	}

	
	static Polygon randomPolygon(){
		int npoints = 3+(int)(Math.random()*4);
		Polygon p = new Polygon();
		for (int i=0;i<npoints;i++) {
			int x = (int)(Math.random()*45);
			int y = (int)(Math.random()*45);
			p.addPoint(x, y);
		}
		return p;
	}
	
	public static void showAt0s() throws Exception{
		File data = new File("data");
		String[] list = data.list();
		File fasa= new File("data/0s/");
		fasa.mkdir();
		for (String f : list) {
//			System.ot.println(f);
			if (f.equals("0s")) continue;
			if (f.substring(f.length()-5).equals("class")) {
				FileInputStream fi = new FileInputStream("data/"+f); 
				ObjectInputStream obj = new ObjectInputStream(fi);
				Object o = obj.readObject();
				String nf = f.substring(0,f.length()-5)+"tstr";
				PrintWriter pw = new PrintWriter("data/0s/"+nf);
				pw.print(o.toString());
				pw.close();
			}
				
		}
	}
	
	static class idtc {
		String strTableName,strClusteringKey;
		Hashtable<String, String> htblColNameType;
		Hashtable<String, Object> htblColNameValue;
		boolean result;
		
		public idtc(String strTableName, String strClusteringKey, Hashtable<String, String> htblColNameType,
				Hashtable<String, Object> htblColNameValue, boolean result) {
			this.strTableName = strTableName;
			this.strClusteringKey = strClusteringKey;
			this.htblColNameType = htblColNameType;
			this.htblColNameValue = htblColNameValue;
			this.result = result;
		}

		void execute() throws DBAppException{
//			clear();
			DBApp d = new DBApp();
			d.init();
//			if(d.exists(strTableName))
//				d.dropTable(strTableName);
			d.createTable(strTableName, strClusteringKey, htblColNameType);
			Table t = DBApp.deserialize(strTableName);
			Vector meta = DBApp.readFile("data/metadata.csv");
			Vector<String[]> metaOfTable = new Vector();
			for (Object o : meta) {
				String[] line = (String[]) o;
				if (line[0].equals(strTableName)) {
					metaOfTable.add(line);
				}
			}
			System.out.println(t.invalidDelete(htblColNameValue, metaOfTable)==result?"Pass":"Fail");
		}
	}
	static void tstInvalidDelete() throws DBAppException{
		ArrayList<idtc> testCases = new ArrayList<>();
		tstInvalidDeleteSchema1(testCases);
		tstInvalidDeleteSchema2(testCases);
		System.out.println("Tesing invalidDelete method: ");
		for (int i=0;i<testCases.size();i++) {
			idtc cur = testCases.get(i);
			System.out.print("Test#"+i+" ");
			cur.execute();
//			cur=null;
		}
	}
	static void tstInvalidDeleteSchema1(ArrayList<idtc> testCases) throws DBAppException{
		Hashtable<String, String> htblColNameType = new Hashtable<>();
		htblColNameType.put("id", "java.lang.Integer");
		String strTableName = "Tab_",strClusteringKey="id";
		
		Hashtable<String, Object> htblColNameValue = new Hashtable<>();
		int i=0;
		htblColNameValue= new Hashtable<>();
		htblColNameValue.put("id", 7);
		testCases.add(new idtc(strTableName+(i++), strClusteringKey, htblColNameType, htblColNameValue, false));
		
		htblColNameValue= new Hashtable<>();
		htblColNameValue.put("id", 5.0);
		testCases.add(new idtc(strTableName+(i++), strClusteringKey, htblColNameType, htblColNameValue, true));
		
		htblColNameValue= new Hashtable<>();
		htblColNameValue.put("id ", 3);
		testCases.add(new idtc(strTableName+(i++), strClusteringKey, htblColNameType, htblColNameValue, true));
		
		htblColNameValue= new Hashtable<>();
		htblColNameValue.put("id", 4);
		testCases.add(new idtc(strTableName+(i++), strClusteringKey, htblColNameType, htblColNameValue, false));
		
	}
	
	static void tstInvalidDeleteSchema2(ArrayList<idtc> testCases) throws DBAppException{
		Hashtable<String, String> htblColNameType = new Hashtable<>();
		htblColNameType.put("x", "java.lang.Integer");
		htblColNameType.put("y", "java.lang.Integer");
		htblColNameType.put("z", "java.lang.Double");
		htblColNameType.put("s", "java.lang.String");
		htblColNameType.put("b", "java.lang.Boolean");
		htblColNameType.put("d", "java.util.Date");
		htblColNameType.put("p", "java.awt.Polygon");
		String strTableName = "Tabl_",strClusteringKey="id";
		
		Hashtable<String, Object> htblColNameValue = new Hashtable<>();
		boolean result; int i=0;
		
		htblColNameValue= new Hashtable<>();
		htblColNameValue.put("x", 4);
		result= false; 
		testCases.add(new idtc(strTableName+(i++), strClusteringKey, htblColNameType, htblColNameValue, result));
		
		htblColNameValue= new Hashtable<>();
		htblColNameValue.put("y", 9);
		result= false; 
		testCases.add(new idtc(strTableName+(i++), strClusteringKey, htblColNameType, htblColNameValue, result));
		
		htblColNameValue= new Hashtable<>();
		htblColNameValue.put("z", 0);	// our database engine doesn't typecast an int to double to suit the table specifications 
		result= true; 
		testCases.add(new idtc(strTableName+(i++), strClusteringKey, htblColNameType, htblColNameValue, result));
		
		htblColNameValue= new Hashtable<>();
		htblColNameValue.put("d", DBApp.parseDate("2000-02-24"));
		result= false; 
		testCases.add(new idtc(strTableName+(i++), strClusteringKey, htblColNameType, htblColNameValue, result));
		
		htblColNameValue= new Hashtable<>();
		htblColNameValue.put("x", 8);
		htblColNameValue.put("y", 4);
		result= false; 
		testCases.add(new idtc(strTableName+(i++), strClusteringKey, htblColNameType, htblColNameValue, result));
		
		htblColNameValue= new Hashtable<>();
		htblColNameValue.put("x", 0.0);
		htblColNameValue.put("y", 4);
		result= true; 
		testCases.add(new idtc(strTableName+(i++), strClusteringKey, htblColNameType, htblColNameValue, result));
		
		htblColNameValue= new Hashtable<>();
		htblColNameValue.put("x", 8);
		htblColNameValue.put("y", 3.2);
		result= true; 
		testCases.add(new idtc(strTableName+(i++), strClusteringKey, htblColNameType, htblColNameValue, result));
		
		htblColNameValue= new Hashtable<>();
		htblColNameValue.put("x", 8);
		htblColNameValue.put("z", (double)5);
		result= false; 
		testCases.add(new idtc(strTableName+(i++), strClusteringKey, htblColNameType, htblColNameValue, result));
		
		htblColNameValue= new Hashtable<>();
		htblColNameValue.put("z", (double) 5);
		htblColNameValue.put("x", 8);
		result= false; 
		testCases.add(new idtc(strTableName+(i++), strClusteringKey, htblColNameType, htblColNameValue, result));
		
		htblColNameValue= new Hashtable<>();
		htblColNameValue.put("d", DBApp.parseDate("1999-05-19"));
		result= false; 
		testCases.add(new idtc(strTableName+(i++), strClusteringKey, htblColNameType, htblColNameValue, result));
		
		htblColNameValue= new Hashtable<>();
		htblColNameValue.put("d", "24-01-2010");
		result= true; 
		testCases.add(new idtc(strTableName+(i++), strClusteringKey, htblColNameType, htblColNameValue, result));
		
		htblColNameValue= new Hashtable<>();
		htblColNameValue.put("p", Polygons.parsePolygon("(0,0),(1,1),(2,2)"));
		result= false; 
		testCases.add(new idtc(strTableName+(i++), strClusteringKey, htblColNameType, htblColNameValue, result));
		
		htblColNameValue= new Hashtable<>();
		result= false; //TODO:
		testCases.add(new idtc(strTableName+(i++), strClusteringKey, htblColNameType, htblColNameValue, result));
		
		htblColNameValue= new Hashtable<>();
		htblColNameValue.put("p", Polygons.parsePolygon("(0,0),(1,1),(0,1)"));
		htblColNameValue.put("z", 2.0);
		result= false; 
		testCases.add(new idtc(strTableName+(i++), strClusteringKey, htblColNameType, htblColNameValue, result));
		
		htblColNameValue= new Hashtable<>();
		htblColNameValue.put("p", Polygons.parsePolygon("(0,0),(1,1),(0,1)"));
		htblColNameValue.put("z", "2.0");
		result= true; 
		testCases.add(new idtc(strTableName+(i++), strClusteringKey, htblColNameType, htblColNameValue, result));
		
		htblColNameValue= new Hashtable<>();
		htblColNameValue.put("p", new Polygon());
		htblColNameValue.put("z", "2.0");
		result= true; 
		testCases.add(new idtc(strTableName+(i++), strClusteringKey, htblColNameType, htblColNameValue, result));
		
		htblColNameValue= new Hashtable<>();
		htblColNameValue.put("p", "(0,0),(1,1),(0,1)");
		htblColNameValue.put("z", 3);
		result= true; 
		testCases.add(new idtc(strTableName+(i++), strClusteringKey, htblColNameType, htblColNameValue, result));
		
		htblColNameValue= new Hashtable<>();
		htblColNameValue.put("s", "11");
		result= false; 
		testCases.add(new idtc(strTableName+(i++), strClusteringKey, htblColNameType, htblColNameValue, result));
		
		htblColNameValue= new Hashtable<>();
		htblColNameValue.put("s", 4);
		result= true; 
		testCases.add(new idtc(strTableName+(i++), strClusteringKey, htblColNameType, htblColNameValue, result));
		
		htblColNameValue= new Hashtable<>();
		htblColNameValue.put("b", true);
		result= false; 
		testCases.add(new idtc(strTableName+(i++), strClusteringKey, htblColNameType, htblColNameValue, result));
		
		htblColNameValue= new Hashtable<>();
		htblColNameValue.put("b", false);
		result= false; 
		testCases.add(new idtc(strTableName+(i++), strClusteringKey, htblColNameType, htblColNameValue, result));
		
		htblColNameValue= new Hashtable<>();
		htblColNameValue.put("b", new Boolean(true));
		result= false; 
		testCases.add(new idtc(strTableName+(i++), strClusteringKey, htblColNameType, htblColNameValue, result));
		
		htblColNameValue= new Hashtable<>();
		htblColNameValue.put("b", new Boolean(false));
		result= false; 
		testCases.add(new idtc(strTableName+(i++), strClusteringKey, htblColNameType, htblColNameValue, result));
		
		htblColNameValue= new Hashtable<>();
		htblColNameValue.put("nonexistingcolumn", 19312);
		result= true; 
		testCases.add(new idtc(strTableName+(i++), strClusteringKey, htblColNameType, htblColNameValue, result));
	}
}
