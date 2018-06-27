package org.frame.hadoop.sql.hbase;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.frame.hadoop.sql.hbase.model.Page;

public interface IHBase {

	public static final String HBASE_FAMILY = "family";
	
	public static final String HBASE_QUALIFIER = "qualifier";
	
	public static final String HBASE_ROWKEY = "key";
	
	public static final String HBASE_TIMESTAMP = "timestamp";
	
	public static final String HBASE_VALUE = "value";
	
	public static final String HBASE_OPERATE = "operate";
	
	public static final CompareOp HBASE_SELECT_EQUAL = CompareOp.EQUAL;
	
	public static final CompareOp HBASE_SELECT_GREATER = CompareOp.GREATER;
	
	public static final CompareOp HBASE_SELECT_GREATER_OR_EQUAL = CompareOp.GREATER_OR_EQUAL;
	
	public static final CompareOp HBASE_SELECT_GREATER_OR_LESS = CompareOp.LESS;
	
	public static final CompareOp HBASE_SELECT_GREATER_OR_LESS_OR_EQUAL = CompareOp.LESS_OR_EQUAL;
	
	public static final CompareOp HBASE_SELECT_GREATER_OR_NO_OP = CompareOp.NO_OP;
	
	public static final CompareOp HBASE_SELECT_NOT_EQUAL = CompareOp.NOT_EQUAL;
	
	/**
	 * create a table
	 * 
	 * @param name name of the table
	 * @param columns columns of the table
	 * 
	 * @return  true function process successfully
	 *         false function process with errors
	 */
    public boolean creat(String name, String[] columns);
    
    public boolean creat(String name, List<String> columns);
    
    public boolean delete(String name, String key);
    
    public boolean delete(String name, String key, String family, String qualifier);
    
    public boolean delete(String name, List<Map<String, String>> parameters);
    
    public boolean delete(Object object);
    
    public boolean delete(Object[] parameters);
    
    public boolean delete(List<Object> parameters);
    
    /** 
     * drop a table
     * 
     * @param name name of the table
     * 
     * @return  true function process successfully
	 *         false function process with errors
     */  
    public boolean drop(String name);
	
    public Map<String, String> find(String name, String key);
    
    public Map<String, String> find(String name, String key, String family, String qualifier);
    
    public Map<String, String> find(String name, String key, List<Map<String, String>> parameters);
    
    public Object find(Object object);
    
    public boolean insert(String key, String name, String family, String qualifier, String value);
    
    public boolean insert(String name, List<Map<String, String>> parameters);
    
    public boolean insert(Object object);
    
    public boolean insert(Object[] parameters);
    
    public boolean insert(List<Object> parameters);
    
    /**
	 * pagination operation
	 * 
	 * @param sql sql for pagination operation
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(String table, Page page);

	/**
	 * pagination operation
	 * 
	 * @param sql sql for pagination operation
	 * @param clazz class of java bean to load data
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(String table, Class<?> clazz, Page page);

	/**
	 * pagination operation
	 * 
	 * @param sql sql for pagination operation
	 * @param parameters parameters for sql
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(String table, List<Map<String, Object>> parameters, Page page);
	
	/**
	 * pagination operation
	 * 
	 * @param sql sql for pagination operation
	 * @param clazz class of java bean to load data
	 * @param parameters parameters for sql
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(String table, List<Map<String, Object>> parameters, Class<?> clazz, Page page);
	
	/**
	 * pagination operation
	 * 
	 * @param sql sql for pagination operation
	 * @param targetPage number of target page
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(String table, int targetPage, Page page);

	/**
	 * pagination operation
	 * 
	 * @param sql sql for pagination operation
	 * @param targetPage number of target page
	 * @param parameters parameters for sql
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(String table, List<Map<String, Object>> parameters, int targetPage, Page page);
	
	/**
	 * pagination operation
	 * 
	 * @param sql sql for pagination operation
	 * @param targetPage number of target page
	 * @param clazz class of java bean to load data
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(String table, Class<?> clazz, int targetPage, Page page);

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
	public Page pagination(String table, List<Map<String, Object>> parameters, Class<?> clazz, int targetPage, Page page);
	
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
	 * @param page instance of org.frame.hadoop.sql.hbase.model.Page
	 * 
	 * @return instance of org.frame.hadoop.sql.hbase.model.Page
	 */
	public Page pagination(Page page);

	/**
	 * pagination operation
	 * 
	 * @param page instance of org.frame.hadoop.sql.hbase.model.Page
	 * @param clazz class of java bean to load data
	 * 
	 * @return instance of org.frame.hadoop.sql.hbase.model.Page
	 */
	public Page pagination(Class<?> clazz, Page page);
    
    public List<Map<String, String>> select(String name);
    
    public List<Map<String, String>> select(String name, String family, String qualifier, String value, CompareOp operate);
    
    public List<Object> select(String name, String family, String qualifier, String value, CompareOp operate, Class<?> clazz);
    
    public List<Map<String, String>> select(String name, List<Map<String, Object>> parameters);
    
    public List<Object> select(String name, List<Map<String, Object>> parameters, Class<?> clazz);
    
    public List<Object> select(String name, Class<?> clazz);
    
    public boolean turncate(String name, String key);
    
    public boolean update(String name, String key, String family, String qualifier, String value);
    
    public boolean update(String name, List<Map<String, String>> parameters);
    
    public boolean update(Object object);
    
    public boolean update(Object[] parameters);
    
    public boolean update(List<Object> parameters);
    
}
