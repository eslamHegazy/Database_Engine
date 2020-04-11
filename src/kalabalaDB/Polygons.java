package kalabalaDB;

import java.awt.Dimension;
import java.awt.Polygon;

public class Polygons extends Polygon implements Comparable<Polygons>{
	public Polygons() {
		super();
	}
	public Polygons(Polygon p) {
		super();
		for (int i=0;i<p.npoints;i++) {
			addPoint(p.xpoints[i], p.ypoints[i]);
		}
	}
	
	public String toString() {
		return ""+area();
//		StringBuilder sb = new StringBuilder();
//		for (int i=0;i<npoints;i++) {
//			if (i>0) {sb.append((","));}
//			sb.append(String.format("(%d,%d)",xpoints[i],ypoints[i]));
//		}
//		return sb.toString();
	}
	public static Polygons parsePolygons(String inp) {
		char[] arr = inp.toCharArray();
		Polygons res = new Polygons();
		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < arr.length; i++) {
			sb = new StringBuilder();
			if (arr[i] == '(' || arr[i] == ',' || arr[i] == ' ')
				continue;
			while (arr[i] != ',') {
				sb.append(arr[i++]);
			}
			int x = Integer.parseInt(sb.toString());
			sb = new StringBuilder();
			i++; // to skip the ',' start the y coordinate
			while (arr[i] != ')') {
				sb.append(arr[i++]);
			}
			int y = Integer.parseInt(sb.toString());
			i++; // to skip the ')'
			res.addPoint(x, y);
		}
		return res;
	}
	public static Polygon parsePolygon(String inp) {
		char[] arr = inp.toCharArray();
		Polygon res = new Polygon();
		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < arr.length; i++) {
			sb = new StringBuilder();
			if (arr[i] == '(' || arr[i] == ',' || arr[i] == ' ')
				continue;
			while (arr[i] != ',') {
				sb.append(arr[i++]);
			}
			int x = Integer.parseInt(sb.toString());
			sb = new StringBuilder();
			i++; // to skip the ',' start the y coordinate
			while (arr[i] != ')') {
				sb.append(arr[i++]);
			}
			int y = Integer.parseInt(sb.toString());
			i++; // to skip the ')'
			res.addPoint(x, y);
		}
		return res;
	}

	public long area() {
		Dimension dim = getBounds().getSize();
		long area = dim.width * dim.height;
		return area;
	}
	public int compareTo(Polygons o) {
		long area = area();
		long areaO = o.area();
		if (area>areaO) {
			return 1;
		}
		else if (area<areaO){
			return -1;
		}
		return 0;
	}
	public boolean equals(Object o) {
		Polygons p = (Polygons) o;
//		if (p==null ) return false;
		if (npoints!=p.npoints) return false;
		for (int i=0;i<npoints;i++) {
			if (xpoints[i]!=p.xpoints[i]) return false;
			if (ypoints[i]!=p.ypoints[i]) return false;
		}
		return true;
	}
	public static void main(String[] args) {
		Polygon p1 = new Polygon();
		Polygon p2 = new Polygon();
		Polygon p3 = new Polygon();
		p1.addPoint(0, 0);
		p2.addPoint(0, 0);
		
		p3.addPoint(5, 0);
		p1.addPoint(5, 0);
		p2.addPoint(5, 0);
		
		p3.addPoint(5, 5);
		p1.addPoint(5, 5);
		p2.addPoint(5, 5);
		
		p3.addPoint(5, 0);
		p1.addPoint(5, 0);
		p2.addPoint(5, 0);
		
		p3.addPoint(0, 0);
		
		System.out.println(p1.equals(p2));
		System.out.println(p1.equals(p3));
		System.out.println(p2.equals(p3));
	}
}
