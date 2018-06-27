/**
 * IJDBCDao contains method for jdbc operation
 */
package org.frame.hadoop.sql.hive;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

import org.frame.repository.sql.model.Page;


public interface IHive {

	public static final String HIVE_TABLEINFO = "table_info";

	public static final String HIVE_SELECTSTATEMENT = "select_statement";

	public static final String HIVE_OVERWRITE = "overwrite";

	public static final String HIVE_PATH = "path";

	public static final String HIVE_LOCAL = "local";

	public boolean execute(String sql);

	/**
	 * find operation(select with id)
	 * 
	 * @param sql sql for find operation
	 * 
	 * @return map of data
	 */
	public Map<String, Object> find(String sql);

	/**
	 * find operation(select with id)
	 * 
	 * @param object instance of java bean to be operated
	 * 
	 * @return instance of java bean(fill with data)
	 */
	public Object find(Object object);

	/**
	 * find operation(select with id)
	 * 
	 * @param sql sql for find operation
	 * @param clazz class of java bean to load data
	 * 
	 * @return instance of java bean
	 */
	public Object find(String sql, Class<?> clazz);

	/**
	 * find operation(select with id)
	 * 
	 * @param sql sql for delete operation
	 * @param parameters parameters for sql
	 * 
	 * @return map of data
	 */
	public Map<String, Object> find(String sql, Map<String, Object> parameters);

	/**
	 * find operation(select with id)
	 * 
	 * @param sql sql for delete operation
	 * @param parameters parameters for sql
	 * 
	 * @return map of data
	 */
	public Map<String, Object> find(String sql, Object[] parameters);

	/**
	 * find operation(select with id)
	 * 
	 * @param sql sql for delete operation
	 * @param clazz class of java bean to load data
	 * @param parameters parameters for sql
	 * 
	 * @return instance of java bean
	 */
	public Object find(String sql, Map<String, Object> parameters, Class<?> clazz);

	/**
	 * find operation(select with id)
	 * 
	 * @param sql sql for delete operation
	 * @param clazz class of java bean to load data
	 * @param parameters parameters for sql
	 * 
	 * @return instance of java bean
	 */
	public Object find(String sql, Object[] parameters, Class<?> clazz);

	public Connection getConn();

	/**
	 * insert operation
	 * 
	 * @param sql sql for insert operation
	 * @param parameters parameters for sql
	 * 
	 * @return number of effect records
	 */
	public boolean emport(String sql);

	/**
	 * insert operation
	 * 
	 * @param sql sql for insert operation
	 * @param parameters parameters for sql
	 * 
	 * @return number of effect records
	 */
	public boolean emport(String tableInfo, String selectStatement, boolean overwrite);

	public boolean emport(String sourceTable, List<Map<String, Object>> parameters);

	public boolean export(String sql);

	public boolean export(String path, String selectStatement, boolean local);

	public boolean export(String sourceTable, List<Map<String, Object>> parameters);

	public boolean load(String sql);

	public boolean load(String path , String tableInfo, boolean local, boolean overwrite);


	/**
	 * pagination operation
	 * 
	 * @param sql sql for pagination operation
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(String sql, Page page);
	/**
	 * pagination operation
	 * 
	 * @param sql sql for pagination operation
	 * @param clazz class of java bean to load data
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(String sql, Class<?> clazz, Page page);

	/**
	 * pagination operation
	 * 
	 * @param sql sql for pagination operation
	 * @param parameters parameters for sql
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(String sql, Map<String, Object> parameters, Page page);

	/**
	 * pagination operation
	 * 
	 * @param sql sql for pagination operation
	 * @param parameters parameters for sql
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(String sql, Object[] parameters, Page page);

	/**
	 * pagination operation
	 * 
	 * @param sql sql for pagination operation
	 * @param clazz class of java bean to load data
	 * @param parameters parameters for sql
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(String sql, Map<String, Object> parameters, Class<?> clazz, Page page);

	/**
	 * pagination operation
	 * 
	 * @param sql sql for pagination operation
	 * @param clazz class of java bean to load data
	 * @param parameters parameters for sql
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(String sql, Object[] parameters, Class<?> clazz, Page page);

	/**
	 * pagination operation
	 * 
	 * @param sql sql for pagination operation
	 * @param targetPage number of target page
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(String sql, int targetPage, Page page);

	/**
	 * pagination operation
	 * 
	 * @param sql sql for pagination operation
	 * @param targetPage number of target page
	 * @param parameters parameters for sql
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(String sql, Map<String, Object> parameters, int targetPage, Page page);

	/**
	 * pagination operation
	 * 
	 * @param sql sql for pagination operation
	 * @param targetPage number of target page
	 * @param parameters parameters for sql
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(String sql, Object[] parameters, int targetPage, Page page);

	/**
	 * pagination operation
	 * 
	 * @param sql sql for pagination operation
	 * @param targetPage number of target page
	 * @param clazz class of java bean to load data
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(String sql, Class<?> clazz, int targetPage, Page page);

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
	public Page pagination(String sql, Map<String, Object> parameters, Class<?> clazz, int targetPage, Page page);

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
	public Page pagination(String sql, Object[] parameters, Class<?> clazz, int targetPage, Page page);

	/**
	 * pagination operation
	 * 
	 * @param page instance of org.frame.model.system.page.Page
	 * @param targetPage number of target page
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(int targetPage, Page page);

	/**
	 * pagination operation
	 * 
	 * @param page instance of org.frame.model.system.page.Page
	 * @param targetPage number of target page
	 * @param clazz class of java bean to load data
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(Class<?> clazz, int targetPage, Page page);

	/**
	 * pagination operation
	 * 
	 * @param page instance of org.frame.model.system.page.Page
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(Page page);

	/**
	 * pagination operation
	 * 
	 * @param page instance of org.frame.model.system.page.Page
	 * @param clazz class of java bean to load data
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(Class<?> clazz, Page page);

	/**
	 * select operation
	 * 
	 * @param sql sql for select operation
	 * 
	 * @return list of data
	 */
	public List<?> select(String sql);

	/**
	 * select operation
	 * 
	 * @param sql sql for select operation
	 * @param clazz class of java bean to load data
	 * 
	 * @return list of data
	 */
	public List<?> select(String sql, Class<?> clazz);

	/**
	 * select operation
	 * 
	 * @param sql sql for select operation
	 * @param parameters parameters for sql
	 * 
	 * @return list of data
	 */
	public List<?> select(String sql, Map<String, Object> parameters);

	/**
	 * select operation
	 * 
	 * @param sql sql for select operation
	 * @param parameters parameters for sql
	 * 
	 * @return list of data
	 */
	public List<?> select(String sql, Object[] parameters);

	/**
	 * select operation
	 * 
	 * @param sql sql for select operation
	 * @param clazz class of java bean to load data
	 * @param parameters parameters for sql
	 * 
	 * @return list of data
	 */
	public List<?> select(String sql, Map<String, Object> parameters, Class<?> clazz);

	/**
	 * select operation
	 * 
	 * @param sql sql for select operation
	 * @param clazz class of java bean to load data
	 * @param parameters parameters for sql
	 * 
	 * @return list of data
	 */
	public List<?> select(String sql, Object[] parameters, Class<?> clazz);

	public void setConn(Connection conn);

}
