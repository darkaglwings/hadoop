package org.frame.hadoop.sql.hbase;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.frame.common.lang.reflect.Reflect;
import org.frame.hadoop.annotation.repository.Column;
import org.frame.hadoop.annotation.repository.Mirror;
import org.frame.hadoop.sql.hbase.impl.HBase;
import org.frame.repository.exception.IlleagalDataException;


public class Annotation {

	/**
	 * check number if legal
	 * 
	 * @param object instance of java bean to be operated
	 * @param scale length of integer part of number
	 * @param precision length of fractional part of number
	 * 
	 * @return  true number is legal
	 *         false number is illegal
	 */
	public boolean checkNumber(Object object, int scale, int precision) {
		return new org.frame.repository.annotation.resolver.Resolver().checkNumber(object, scale, precision);
	}
	
	/**
	 * check string if legal
	 * 
	 * @param object instance of java bean to be operated
	 * @param length length of string
	 * 
	 * @return  true string is legal
	 *         false string is illegal
	 */
	public boolean checkString(Object object, int length) {
		return new org.frame.repository.annotation.resolver.Resolver().checkString(object, length);
	}
	
	/**
	 * analyze parameters for operation in object
	 * 
	 * @param repository instance of org.frame.repository.IRepository
	 * @param object instance of java bean to be operated
	 * @param type parameter analysis mode(delete, insert, select, update)
	 * 
	 * @return map of parameters
	 *         option keys:   id id parameters in object
	 *                      data parameters in object
	 * 
	 * @throws IlleagalDataException if data in object is illegal
	 */
	public List<Map<String, String>> parameters(Object object) {
		List<Map<String, String>> result = null;

		if (object != null) {
			result = new ArrayList<Map<String, String>>();

			try {
				Class<?> clazz = object.getClass();

				Reflect reflect = new Reflect();

				boolean isSimple = this.simple(clazz);

				Map<String, String> map = null;

				Field[] fields = clazz.getDeclaredFields();
				if (isSimple) {
					for (Field field : fields) {
						if (!"id".equals(field.getName())) {
							map = new HashMap<String, String>();
							map.put(HBase.HBASE_ROWKEY, String.valueOf(reflect.get(object, "id")));
							map.put(HBase.HBASE_FAMILY, field.getName());
							map.put(HBase.HBASE_QUALIFIER, field.getName());
							map.put(HBase.HBASE_VALUE, String.valueOf(reflect.get(object, field.getName().toLowerCase())));

							result.add(map);
						}
					}
				} else {
					String key = null;
					for (Field field : fields) {
						if (field.isAnnotationPresent(Column.class)) {
							Column column = field.getAnnotation(Column.class);

							if (column.primary()) {
								key = String.valueOf(reflect.get(object, field.getName().toLowerCase()));
							}
						}
					}

					for (Field field : fields) {
						if (field.isAnnotationPresent(Column.class)) {
							Column column = field.getAnnotation(Column.class);

							if (!column.primary()) {
								map = new HashMap<String, String>();
								map.put(HBase.HBASE_ROWKEY, key);
								map.put(HBase.HBASE_FAMILY, column.family());
								map.put(HBase.HBASE_QUALIFIER, column.qualifier());
								map.put(HBase.HBASE_VALUE, String.valueOf(reflect.get(object, field.getName().toLowerCase())));

								result.add(map);
							}
						}

						if (field.isAnnotationPresent(Mirror.class)) {
							Mirror mirror = field.getAnnotation(Mirror.class);

							map = new HashMap<String, String>();
							map.put(HBase.HBASE_ROWKEY, key);
							map.put(HBase.HBASE_FAMILY, mirror.family());
							map.put(HBase.HBASE_QUALIFIER, mirror.qualifier());
							map.put(HBase.HBASE_VALUE, String.valueOf(reflect.get(object, field.getName().toLowerCase())));

							result.add(map);
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return result;
	}
	
	public Map<String, Object> pk(Object object) {
		return new org.frame.repository.annotation.resolver.Resolver().pk(object);
	}
	
	/**
	 * to get analyze mode
	 * 
	 * @param clazz class of java bean to be operated
	 * 
	 * @return  true use simple mode
	 *         false not use simple mode
	 */
	public boolean simple(Class<?> clazz) {
		return new org.frame.repository.annotation.resolver.Resolver().simple(clazz);
	}
	
	/**
	 * to get table name
	 * 
	 * @param clazz class of java bean to be operated
	 * 
	 * @return string of table name
	 */
	public String table(Class<?> clazz) {
		return new org.frame.repository.annotation.resolver.Resolver().table(clazz);
	}
}
