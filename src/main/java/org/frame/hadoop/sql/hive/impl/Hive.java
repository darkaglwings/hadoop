/** 
 * Hive Java API
 * hive remote service interface required: hive --service hiveserver & 
 * 
 * HIVE DDL:
 *   CREATE [EXTERNAL] TABLE [IF NOT EXISTS] table_name 
 *    [(col_name data_type [COMMENT col_comment], ...)] 
 *    [COMMENT table_comment] 
 *    [PARTITIONED BY (col_name data_type [COMMENT col_comment], ...)] 
 *    [CLUSTERED BY (col_name, col_name, ...) 
 *    [SORTED BY (col_name [ASC|DESC], ...)] INTO num_buckets BUCKETS] 
 *    [ROW FORMAT row_format] 
 *    [STORED AS file_format] 
 *    [LOCATION hdfs_path]
 * 
 *   DROP TABLE tablename
 * 
 * HIVE SQL:
 *   LOAD DATA [LOCAL] INPATH 'filepath' [OVERWRITE] INTO TABLE tablename [PARTITION (partcol1=val1, partcol2=val2 ...)]
 * 
 *   INSERT OVERWRITE [LOCAL] DIRECTORY directory1 select_statement1
 * 
 *   SELECT [ALL | DISTINCT] select_expr, select_expr, ... FROM table_reference
 *    [WHERE where_condition]
 *    [GROUP BY col_list [HAVING condition]]
 *    [CLUSTER BY col_list | [DISTRIBUTE BY col_list] [SORT BY| ORDER  BY col_list]]
 *    [LIMIT number]
 * 
 */
package org.frame.hadoop.sql.hive.impl;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import org.frame.hadoop.sql.hive.IHive;
import org.frame.repository.constant.IRepositoryConstant;
import org.frame.repository.sql.jdbc.impl.JDBC;
import org.frame.repository.sql.model.Page;

public class Hive implements IHive {
	
	private JDBC jdbc = null;
	
	/**
	 * constructor with default configuration
	 */
	public Hive() {
		this.jdbc = new JDBC(IRepositoryConstant.DEFAULT_CONFIG_PROPERTIES);
	}
	
	/**
	 * constructor with specific configuration
	 * 
	 * @param config file path of configuration file
	 */
	public Hive(String config) {
		this.jdbc = new JDBC(config);
	}
	
	public Hive(Connection conn) {
		this.jdbc = new JDBC(conn);
	}
	
	/**
	 * constructor with specific configuration
	 * 
	 * @param file configuration file
	 */
	public Hive(File file) {
		this.jdbc = new JDBC(file);
	}
	
	public Hive(String className, String url, String username, String password) {
		this.jdbc = new JDBC(className, url, username, password);
	}
	
	public boolean execute(String sql) {
		boolean result = false;
		if (this.jdbc != null) {
			Connection conn = null;
			Statement stmt = null;
			try {
				conn = this.jdbc.connect();
				stmt = conn.createStatement();
				stmt.executeQuery(sql);
				
				result = true;
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				this.jdbc.destory(conn, stmt, null, null, null);
			}
			
		} else {
			System.err.println("can not progress hive operation: jdbc can not be created.");
		}
		
		return result;
	}
	
	/**
	 * find operation(select with id)
	 * 
	 * @param sql sql for find operation
	 * 
	 * @return map of data
	 */
	public Map<String, Object> find(String sql) {
		return this.jdbc.find(sql);
	}
	
	/**
	 * find operation(select with id)
	 * 
	 * @param object instance of java bean to be operated
	 * 
	 * @return instance of java bean(fill with data)
	 */
	public Object find(Object object) {
		return this.find(object);
	}

	/**
	 * find operation(select with id)
	 * 
	 * @param sql sql for find operation
	 * @param clazz class of java bean to load data
	 * 
	 * @return instance of java bean
	 */
	public Object find(String sql, Class<?> clazz) {
		return this.find(sql, clazz);
	}
	
	/**
	 * find operation(select with id)
	 * 
	 * @param sql sql for delete operation
	 * @param parameters parameters for sql
	 * 
	 * @return map of data
	 */
	public Map<String, Object> find(String sql, Map<String, Object> parameters) {
		return this.jdbc.find(sql, parameters);
	}
	
	/**
	 * find operation(select with id)
	 * 
	 * @param sql sql for delete operation
	 * @param parameters parameters for sql
	 * 
	 * @return map of data
	 */
	public Map<String, Object> find(String sql, Object[] parameters) {
		return this.jdbc.find(sql, parameters);
	}

	/**
	 * find operation(select with id)
	 * 
	 * @param sql sql for delete operation
	 * @param clazz class of java bean to load data
	 * @param parameters parameters for sql
	 * 
	 * @return instance of java bean
	 */
	public Object find(String sql, Map<String, Object> parameters, Class<?> clazz) {
		return this.jdbc.find(sql, parameters, clazz);
	}
	
	/**
	 * find operation(select with id)
	 * 
	 * @param sql sql for delete operation
	 * @param clazz class of java bean to load data
	 * @param parameters parameters for sql
	 * 
	 * @return instance of java bean
	 */
	public Object find(String sql, Object[] parameters, Class<?> clazz) {
		return this.jdbc.find(sql, parameters, clazz);
	}
	
	public Connection getConn() {
		return this.jdbc.getConn();
	}

	/**
	 * insert operation
	 * 
	 * @param sql sql for insert operation
	 * @param parameters parameters for sql
	 * 
	 * @return number of effect records
	 */
	public boolean emport(String sql) {
		return this.execute(sql);
	}
	
	/**
	 * insert operation
	 * 
	 * @param sql sql for insert operation
	 * @param parameters parameters for sql
	 * 
	 * @return number of effect records
	 */
	public boolean emport(String tableInfo, String selectStatement, boolean overwrite) {
		StringBuffer sbuf = new StringBuffer();
		
		String into;
		if (overwrite) {
			into = "overwrite ";
		} else {
			into = "into ";
		}
		
		sbuf.append("insert ").append(into).append("table ").append(tableInfo).append(" ").append(selectStatement);
		
		return this.execute(sbuf.toString());
	}
	
	public boolean emport(String sourceTable, List<Map<String, Object>> parameters) {
		StringBuffer sbuf = new StringBuffer();
		sbuf.append("from ").append(sourceTable);
		
		boolean overwrite;
		String tableInfo, selectStatement, into;
		for (Map<String, Object> map : parameters) {
			tableInfo = String.valueOf(map.get(Hive.HIVE_TABLEINFO));
			selectStatement = String.valueOf(map.get(Hive.HIVE_SELECTSTATEMENT));
			overwrite = Boolean.parseBoolean(String.valueOf(map.get(Hive.HIVE_OVERWRITE)));
			
			if (overwrite) {
				into = "overwrite ";
			} else {
				into = "into ";
			}
			
			sbuf.append(" insert ").append(into).append("table ").append(tableInfo).append(" ").append(selectStatement);
		}
		
		return this.execute(sbuf.toString());
	}
	
	public boolean export(String sql) {
		return this.execute(sql);
	}
	
	public boolean export(String path, String selectStatement, boolean local) {
		StringBuffer sbuf = new StringBuffer();
		
		String hdfs;
		
		if (local) {
			hdfs = "local ";
		} else {
			hdfs = "";
		}
		
		sbuf.append("insert overwrite ").append(hdfs).append("directory '").append(path).append("' ").append(selectStatement);
		return this.execute(sbuf.toString());
	}
	
	public boolean export(String sourceTable, List<Map<String, Object>> parameters) {
		StringBuffer sbuf = new StringBuffer();
		sbuf.append("from ").append(sourceTable);
		
		boolean local;
		String hdfs, path, selectStatement;
		for (Map<String, Object> map : parameters) {
			path = String.valueOf(map.get(Hive.HIVE_PATH));
			selectStatement = String.valueOf(map.get(Hive.HIVE_SELECTSTATEMENT));
			local = Boolean.parseBoolean(String.valueOf(map.get(Hive.HIVE_LOCAL)));
			
			if (local) {
				hdfs = "local ";
			} else {
				hdfs = "";
			}
			
			sbuf.append(" insert overwrite ").append(hdfs).append("directory '").append(path).append("' ").append(selectStatement);
		}
		
		return this.execute(sbuf.toString());
	}
	
	public boolean load(String sql) {
		return this.execute(sql);
	}
	
	public boolean load(String path , String tableInfo, boolean local, boolean overwrite) {
		StringBuffer sbuf = new StringBuffer();
		
		String hdfs, into;
		
		if (local) {
			hdfs = "local ";
		} else {
			hdfs = "";
		}
		
		if (overwrite) {
			into = "overwrite into ";
		} else {
			into = "into ";
		}
		
		sbuf.append("load data ").append(hdfs).append("inpath '").append(path).append("' ").append(into).append("table ").append(tableInfo);
		return this.execute(sbuf.toString());
	}
	
	
	
	/**
	 * pagination operation
	 * 
	 * @param sql sql for pagination operation
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(String sql, Page page) {
		return this.jdbc.pagination(sql, page);
	}

	/**
	 * pagination operation
	 * 
	 * @param sql sql for pagination operation
	 * @param clazz class of java bean to load data
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(String sql, Class<?> clazz, Page page) {
		return this.jdbc.pagination(sql, clazz, page);
	}

	/**
	 * pagination operation
	 * 
	 * @param sql sql for pagination operation
	 * @param parameters parameters for sql
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(String sql, Map<String, Object> parameters, Page page) {
		return this.jdbc.pagination(sql, parameters, page);
	}
	
	/**
	 * pagination operation
	 * 
	 * @param sql sql for pagination operation
	 * @param parameters parameters for sql
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(String sql, Object[] parameters, Page page) {
		return this.jdbc.pagination(sql, parameters, page);
	}

	/**
	 * pagination operation
	 * 
	 * @param sql sql for pagination operation
	 * @param clazz class of java bean to load data
	 * @param parameters parameters for sql
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(String sql, Map<String, Object> parameters, Class<?> clazz, Page page) {
		return this.jdbc.pagination(sql, parameters, clazz, page);
	}
	
	/**
	 * pagination operation
	 * 
	 * @param sql sql for pagination operation
	 * @param clazz class of java bean to load data
	 * @param parameters parameters for sql
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(String sql, Object[] parameters, Class<?> clazz, Page page) {
		return this.jdbc.pagination(sql, parameters, clazz, page);
	}

	/**
	 * pagination operation
	 * 
	 * @param sql sql for pagination operation
	 * @param targetPage number of target page
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(String sql, int targetPage, Page page) {
		return this.jdbc.pagination(sql, targetPage, page);
	}

	/**
	 * pagination operation
	 * 
	 * @param sql sql for pagination operation
	 * @param targetPage number of target page
	 * @param parameters parameters for sql
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(String sql, Map<String, Object> parameters, int targetPage, Page page) {
		return this.jdbc.pagination(sql, parameters, targetPage, page);
	}
	
	/**
	 * pagination operation
	 * 
	 * @param sql sql for pagination operation
	 * @param targetPage number of target page
	 * @param parameters parameters for sql
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(String sql, Object[] parameters, int targetPage, Page page) {
		return this.jdbc.pagination(sql, parameters, targetPage, page);
	}

	/**
	 * pagination operation
	 * 
	 * @param sql sql for pagination operation
	 * @param targetPage number of target page
	 * @param clazz class of java bean to load data
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(String sql, Class<?> clazz, int targetPage, Page page) {
		return this.jdbc.pagination(sql, clazz, targetPage, page);
	}

	/**
	 * pagination operation
	 * 
	 * @param sql sql for pagination operation
	 * @param targetPage number of target page
	 * @param clazz class of java bean to load data
	 * @param parameters parameters for sql
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(String sql, Map<String, Object> parameters, Class<?> clazz, int targetPage, Page page) {
		return this.jdbc.pagination(sql, parameters, clazz, targetPage, page);
	}
	
	/**
	 * pagination operation
	 * 
	 * @param sql sql for pagination operation
	 * @param targetPage number of target page
	 * @param clazz class of java bean to load data
	 * @param parameters parameters for sql
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(String sql, Object[] parameters, Class<?> clazz, int targetPage, Page page) {
		return this.jdbc.pagination(sql, parameters, clazz, targetPage, page);
	}

	/**
	 * pagination operation
	 * 
	 * @param page instance of org.frame.model.system.page.Page
	 * @param targetPage number of target page
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(int targetPage, Page page) {
		return this.jdbc.pagination(targetPage, page);
	}

	/**
	 * pagination operation
	 * 
	 * @param page instance of org.frame.model.system.page.Page
	 * @param targetPage number of target page
	 * @param clazz class of java bean to load data
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(Class<?> clazz, int targetPage, Page page) {
		return this.jdbc.pagination(clazz, targetPage, page);
	}
	
	/**
	 * pagination operation
	 * 
	 * @param page instance of org.frame.model.system.page.Page
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(Page page) {
		return this.jdbc.pagination(page);
	}

	/**
	 * pagination operation
	 * 
	 * @param page instance of org.frame.model.system.page.Page
	 * @param clazz class of java bean to load data
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(Class<?> clazz, Page page) {
		return this.jdbc.pagination(clazz, page);
	}
	
	/**
	 * select operation
	 * 
	 * @param sql sql for select operation
	 * 
	 * @return list of data
	 */
	public List<?> select(String sql) {
		return this.jdbc.select(sql);
	}

	/**
	 * select operation
	 * 
	 * @param sql sql for select operation
	 * @param clazz class of java bean to load data
	 * 
	 * @return list of data
	 */
	public List<?> select(String sql, Class<?> clazz) {
		return this.jdbc.select(sql, clazz);
	}

	/**
	 * select operation
	 * 
	 * @param sql sql for select operation
	 * @param parameters parameters for sql
	 * 
	 * @return list of data
	 */
	public List<?> select(String sql, Map<String, Object> parameters) {
		return this.jdbc.select(sql, parameters);
	}
	
	/**
	 * select operation
	 * 
	 * @param sql sql for select operation
	 * @param parameters parameters for sql
	 * 
	 * @return list of data
	 */
	public List<?> select(String sql, Object[] parameters) {
		return this.jdbc.select(sql, parameters);
	}
	
	/**
	 * select operation
	 * 
	 * @param sql sql for select operation
	 * @param clazz class of java bean to load data
	 * @param parameters parameters for sql
	 * 
	 * @return list of data
	 */
	public List<?> select(String sql, Map<String, Object> parameters, Class<?> clazz) {
		return this.jdbc.select(sql, parameters, clazz);
	}

	/**
	 * select operation
	 * 
	 * @param sql sql for select operation
	 * @param clazz class of java bean to load data
	 * @param parameters parameters for sql
	 * 
	 * @return list of data
	 */
	public List<?> select(String sql, Object[] parameters, Class<?> clazz) {
		return this.jdbc.select(sql, parameters, clazz);
	}

	public void setConn(Connection conn) {
		this.jdbc.setConn(conn);
	}
	
	/*public static void main(String[] args) {
		String classname = "org.apache.hadoop.hive.jdbc.HiveDriver";
		String url = "jdbc:hive://10.10.10.230:10000/default";
		String username = "";
		String password = "";
				
		Hive hive = new Hive(classname, url, username, password);
		String sql = "select create_date, create_time, info from test where 1 = 1";
		Object[] parameters = {"10:11:05", "10:11:05"};
		
		System.out.println(new Date().toString());
		
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> result = (List<Map<String, Object>>) hive.select(sql);
		for (Map<String, Object> record : result) {
			System.out.print(record.get("create_date"));
			System.out.print(" ");
			System.out.print(record.get("create_time"));
			System.out.print(" ");
			System.out.print(record.get("info"));
			System.out.print("\n");
		}
		
		System.out.println(new Date().toString());
		
		boolean b = false;
		b = hive.load("/user/root/input/test/test.txt", "test", false, true);
		System.out.println("load: " + b);
		
		b = hive.emport("test", "select * from test", false);
		System.out.println("emport: " + b);
		
		b = hive.export("/user/root/input/test/test.txt", "select * from test", false);
		System.out.println("export: " + b);
	}*/

}
