package kalabalaDB;

import java.awt.Dimension;
import java.awt.Polygon;

public class Polygons extends Polygon implements Comparable<Polygons>{
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (int i=0;i<npoints;i++) {
			if (i>0) {sb.append((","));}
			sb.append(String.format("(%d,%d)",xpoints[i],ypoints[i]));
		}
		return sb.toString();
	}
	public static Polygons parsePolygons(String inp) {
		return (Polygons)parsePolygon(inp);
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

}
