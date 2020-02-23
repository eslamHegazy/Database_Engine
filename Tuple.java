package DBV2;
import java.io.*;
import java.util.*;

public class Tuple implements Serializable {
	private Vector attributes = new Vector();

	public void addAttribute(Object o) {
		attributes.add(o);
	}

	public static void main(String[] args) {

	}

	public String toString() {
		String str = "";
		for (Object y : attributes) {
			if (y == null) {
				System.out.println("null");
			} else {

				str += y.toString()+"\t";
			}
		}
		return str;
	}

	public Vector getAttributes() {
		return attributes;
	}

	public void setAttributes(Vector attributes) {
		this.attributes = attributes;
	}
}