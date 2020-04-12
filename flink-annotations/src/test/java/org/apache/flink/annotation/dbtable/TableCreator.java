package org.apache.flink.annotation.dbtable;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2020-04-11
 */
public class TableCreator {

	public static void main(String[] args) throws Exception {

		String className = Member.class.getName();
		Class<?> cl = Class.forName(className);
		DBTable dbtable = cl.getAnnotation(DBTable.class);
		if (dbtable == null) {
			System.out.println("No DbTable annotations in class " + className);
		}

		String tableName = dbtable.name();
		// If the name is empty , use the Class name:
		if (tableName.length() < 1) {
			tableName = cl.getName().toUpperCase();
		}

		List<String> columnDefs = new ArrayList<>();
		for (Field field : cl.getDeclaredFields()) {
			String columnName;
			Annotation[] annotations = field.getDeclaredAnnotations();
			if (annotations.length < 1) {
				continue; // Not a db table column
			}
			if (annotations[0] instanceof SQLInteger) {
				SQLInteger sInt = (SQLInteger) annotations[0];
				// Use field name if name not specified
				if (sInt.name().length() < 1) {
					columnName = field.getName().toUpperCase();
				} else {
					columnName = sInt.name();
				}
				columnDefs.add(columnName + " INT" + getConstraints(sInt.constraints()));
			} else if (annotations[0] instanceof SQLString) {
				SQLString sString = (SQLString) annotations[0];
				// Use field name if name not specified.
				if (sString.name().length() < 1) {
					columnName = field.getName().toUpperCase();
				} else {
					columnName = sString.name();
				}
				columnDefs.add(columnName + " VARCHAR(" + sString.len() + ")" + getConstraints(sString.constraints()));
			}
		}
		StringBuilder createCommand = new StringBuilder("CREATE TABLE " + tableName + "(");
		for (String columnDef : columnDefs) {
			createCommand.append("\n    " + columnDef + ",");
		}
		// Remove trailing comma
		String tableCreate = createCommand.substring(0, (createCommand.length() - 1)) + ");";
		System.out.println("Table.Creation SQL for " + className + " is :\n " + tableCreate);
	}

	private static String getConstraints(Constraints con) {
		String constraints = "";
		if (!con.allowNull()) {
			constraints += " NOT NULL";
		}
		if (con.primaryKey()) {
			constraints += " PRIMARY KEY";
		}
		if (con.unique()) {
			constraints += " UNIQUE";
		}
		return constraints;
	}
}
