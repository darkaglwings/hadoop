package org.frame.hadoop.sql.hbase.impl;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.frame.common.lang.reflect.Reflect;
import org.frame.common.util.Properties;
import org.frame.hadoop.sql.hbase.Annotation;
import org.frame.hadoop.sql.hbase.IHBase;
import org.frame.hadoop.sql.hbase.constant.IHBaseConstant;
import org.frame.hadoop.sql.hbase.model.Page;
import org.frame.repository.annotation.Column;
import org.frame.repository.annotation.Mirror;

@SuppressWarnings("deprecation")
public class HBase implements IHBase {

	private static HConnection conn;
	
	//RPCs = (Rows * Cols per Row) / Min(Cols per Row, Batch Size)/ Scanner Caching
	private int batch = 5;
	
	private int caching = 100;
	
	static {
		Configuration conf = HBaseConfiguration.create();
		
		Properties properties = new Properties(IHBaseConstant.DEFAULT_CONFIG_PROPERTIES);
		
		conf.set("hbase.zookeeper.quorum", properties.read(IHBaseConstant.HBASE_MASTER));
		//conf.set("hbase.zookeeper.property.clientPort", "60000"); 
		//conf.set("hbase.master", master);
		
		try {
			conn = HConnectionManager.createConnection(conf);
		} catch (IOException e) {
			conn = null;
			e.printStackTrace();
		}
	}
	
	/**
	 * create a table
	 * 
	 * @param name name of the table
	 * @param columns columns of the table
	 * 
	 * @return  true function process successfully
	 *         false function process with errors
	 */
    public boolean creat(String name, String[] columns) {
    	boolean result = false;
    	
    	HBaseAdmin admin = null;
    	try {
    		admin = new HBaseAdmin(conn);
            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(name));
            for (String column : columns) {
            	table.addFamily(new HColumnDescriptor(column));
            }
            
            admin.createTable(table);
            result = true;
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			this.destory(admin, null, null);
		}
        
        return result;
    }
    
    public boolean creat(String name, List<String> columns) {
    	return this.creat(name, columns.toArray(new String[columns.size()]));
    }
    
    public boolean delete(String name, String key) {
    	boolean result = false;

    	HTable table = null;
    	try {
    		table = new HTable(TableName.valueOf(name), conn);

    		Delete delete = new Delete(Bytes.toBytes(key));
    		table.delete(delete);

    		result = true;
    	} catch (MasterNotRunningException e) {
    		e.printStackTrace();
    	} catch (ZooKeeperConnectionException e) {
    		e.printStackTrace();
    	} catch (IOException e) {
    		e.printStackTrace();
    	} finally {
    		this.destory(null, table, null);
    	}

    	return result;
    }
    
    public boolean delete(String name, String key, String family, String qualifier) {
    	boolean result = false;

    	HTable table = null;
    	try {
    		table = new HTable(TableName.valueOf(name), conn);
    		Delete delete = new Delete(Bytes.toBytes(key));
    		delete.deleteColumns(Bytes.toBytes(family), Bytes.toBytes(qualifier));
    		table.delete(delete);

    		result = true;
    	} catch (MasterNotRunningException e) {
    		e.printStackTrace();
    	} catch (ZooKeeperConnectionException e) {
    		e.printStackTrace();
    	} catch (IOException e) {
    		e.printStackTrace();
    	} finally {
    		this.destory(null, table, null);
    	}

    	return result;
    }
    
    public boolean delete(String name, List<Map<String, String>> parameters) {
    	boolean result = false;

    	HTable table = null;
    	try {
    		table = new HTable(TableName.valueOf(name), conn);
    		table.setAutoFlush(false, true);
    		//table.setWriteBufferSize(2048);

    		Delete delete;
    		for (Map<String, String> map : parameters) {
    			delete = new Delete(Bytes.toBytes(map.get(HBASE_ROWKEY)));
    			delete.deleteColumns(Bytes.toBytes(map.get(HBASE_FAMILY)), Bytes.toBytes(map.get(HBASE_QUALIFIER)));
    			table.delete(delete);
    		}

    		table.flushCommits();

    		result = true;
    	} catch (MasterNotRunningException e) {
    		e.printStackTrace();
    	} catch (ZooKeeperConnectionException e) {
    		e.printStackTrace();
    	} catch (IOException e) {
    		e.printStackTrace();
    	} finally {
    		this.destory(null, table, null);
    	}
    	
    	return result;
    }
    
    public boolean delete(Object object) {
    	boolean result = false;
    	
    	Annotation annotation = new Annotation();
    	
    	String key, rowKey = null, name = annotation.table(object.getClass());
    	Map<String, Object> map = new org.frame.hadoop.sql.hbase.Annotation().pk(object);
    	for (Iterator<?> iterator = map.keySet().iterator(); iterator.hasNext();) {
			key = (String) iterator.next();
			rowKey = String.valueOf(map.get(key.toLowerCase()));
		}
    	
    	if (rowKey != null) {
    		result = this.delete(name, rowKey);
    	}
    	
    	return result;
    }
    
    public boolean delete(Object[] parameters) {
    	boolean result = true;
    	
    	for (Object object : parameters) {
    		result = result && this.delete(object);
    	}
    	
    	return result;
    }
    
    public boolean delete(List<Object> parameters) {
    	return this.delete(new Object[parameters.size()]);
    }
    
    /** 
     * drop a table
     * 
     * @param name name of the table
     * 
     * @return  true function process successfully
	 *         false function process with errors
     */  
    public boolean drop(String name) {
    	boolean result = false;
    	
    	HBaseAdmin admin = null;
        try {
        	admin = new HBaseAdmin(conn);
        	admin.disableTable(name);
        	admin.deleteTable(name);

        	result = true;
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
        	this.destory(admin, null, null);
        }
  
        return result;
    }
	
    public Map<String, String> find(String name, String key) {
    	Map<String, String> result = null;

    	HTable table = null;
    	try {
    		table = new HTable(TableName.valueOf(name), conn);

    		Get get = new Get(Bytes.toBytes(key));

    		Result r = table.get(get);
    		result = this.rs2Map(r);
    	} catch(IOException e) {
    		e.printStackTrace();
    	} finally {
    		this.destory(null, table, null);
    	}

    	return result;
    }
    
    public Map<String, String> find(String name, String key, String family, String qualifier) {
    	Map<String, String> result = null;

    	HTable table = null;
    	try {
    		table = new HTable(TableName.valueOf(name), conn);
    		Get get = new Get(Bytes.toBytes(key));
    		get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));

    		Result r = table.get(get);
    		result = this.rs2Map(r);
    	} catch(IOException e) {
    		e.printStackTrace();
    	} finally {
    		this.destory(null, table, null);
    	}

    	return result;
    }
    
    public Map<String, String> find(String name, String key, List<Map<String, String>> parameters) {
    	Map<String, String> result = null;

    	HTable table = null;
    	try {
    		table = new HTable(TableName.valueOf(name), conn);

    		Get get = new Get(Bytes.toBytes(key));
    		for (Map<String, String> map : parameters) {
    			get.addColumn(Bytes.toBytes(map.get(HBase.HBASE_FAMILY)), Bytes.toBytes(map.get(HBase.HBASE_QUALIFIER)));
    		}

    		Result r = table.get(get);
    		result = this.rs2Map(r);
    	} catch(IOException e) {
    		e.printStackTrace();
    	} finally {
    		this.destory(null, table, null);
    	}

    	return result;
    }
    
    public Object find(Object object) {
    	Object result = null;

    	Annotation annotation = new Annotation();

    	String key, rowKey = null, name = annotation.table(object.getClass());
    	Map<String, Object> map = new org.frame.hadoop.sql.hbase.Annotation().pk(object);
    	for (Iterator<?> iterator = map.keySet().iterator(); iterator.hasNext();) {
    		key = (String) iterator.next();
    		rowKey = String.valueOf(map.get(key.toLowerCase()));
    	}

    	if (rowKey != null) {
    		HTable table = null;
    		try {
    			table = new HTable(TableName.valueOf(name), conn);

    			Get get = new Get(Bytes.toBytes(rowKey));
    			Result r = table.get(get);
    			result = this.rs2Object(r, object.getClass());
    		} catch(IOException e) {
    			e.printStackTrace();
    		} finally {
    			this.destory(null, table, null);
    		}
    	}

    	return result;
    }
    
    public int getBatch() {
		return batch;
	}
    
    public int getCaching() {
		return caching;
	}
    
    public boolean insert(String key, String name, String family, String qualifier, String value) {
    	return this.update(name, key, family, qualifier, value);
    }
    
    public boolean insert(String name, List<Map<String, String>> parameters) {
    	return this.update(name, parameters);
    }
    
    public boolean insert(Object object) {
    	return this.update(object);
    }
    
    public boolean insert(Object[] parameters) {
    	return this.update(parameters);
    }
    
    public boolean insert(List<Object> parameters) {
    	return this.insert(new Object[parameters.size()]);
    }
    
    /**
	 * pagination operation
	 * 
	 * @param sql sql for pagination operation
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(String table, Page page) {
		return this.pagination(this.initPage(table, null, page));
	}

	/**
	 * pagination operation
	 * 
	 * @param sql sql for pagination operation
	 * @param clazz class of java bean to load data
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(String table, Class<?> clazz, Page page) {
		return this.pagination(clazz, this.initPage(table, null, page));
	}

	/**
	 * pagination operation
	 * 
	 * @param sql sql for pagination operation
	 * @param parameters parameters for sql
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(String table, List<Map<String, Object>> parameters, Page page) {
		return this.pagination(this.initPage(table, parameters, page));
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
	public Page pagination(String table, List<Map<String, Object>> parameters, Class<?> clazz, Page page) {
		return this.pagination(clazz, this.initPage(table, parameters, page));
	}
	
	/**
	 * pagination operation
	 * 
	 * @param sql sql for pagination operation
	 * @param targetPage number of target page
	 * 
	 * @return instance of org.frame.model.system.page.Page
	 */
	public Page pagination(String table, int targetPage, Page page) {
		return this.pagination(targetPage, this.initPage(table, null, page));
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
	public Page pagination(String table, List<Map<String, Object>> parameters, int targetPage, Page page) {
		return this.pagination(targetPage, this.initPage(table, parameters, page));
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
	public Page pagination(String table, Class<?> clazz, int targetPage, Page page) {
		return this.pagination(clazz, targetPage, this.initPage(table, null, page));
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
	public Page pagination(String table, List<Map<String, Object>> parameters, Class<?> clazz, int targetPage, Page page) {
		return this.pagination(clazz, targetPage, this.initPage(table, parameters, page));
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
		if (page != null) {
			page.setTargetPage(targetPage);
			return this.pagination(page);
		} else {
			System.err.println("page is null");
			return new Page();
		}
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
		if (page != null) {
			page.setTargetPage(targetPage);
			return this.pagination(clazz, page);
		} else {
			System.err.println("page is null");
			return new Page();
		}
	}
	
	/**
	 * pagination operation
	 * 
	 * @param page instance of org.frame.hadoop.sql.hbase.model.Page
	 * 
	 * @return instance of org.frame.hadoop.sql.hbase.model.Page
	 */
	public Page pagination(Page page) {
		if (page != null) {
			String table = page.getTable();
			List<Map<String, Object>> parameters = page.getParameters();
			if (parameters == null) {
				parameters = new ArrayList<Map<String, Object>>();
			}
			
			List<Map<String, String>> rowData = null, data = new ArrayList<Map<String, String>>();
			if (table != null) {
				rowData = this.select(table, parameters);
			}
			
			if (rowData != null) {
				page.setTotalCount(rowData.size());
				page.init();
				
				for (int i = page.getStartIndex(); i <= page.getEndIndex(); i++) {
					data.add(rowData.get(i));
				}
			} else {
				page.setTotalCount(0);
				page.init();
			}
			
			page.setData(data);
			page.setCurrPage(page.getTargetPage());
		} else {
			System.err.println("page is null");
			page = new Page();
		}
		
		return page;
	}

	/**
	 * pagination operation
	 * 
	 * @param page instance of org.frame.hadoop.sql.hbase.model.Page
	 * @param clazz class of java bean to load data
	 * 
	 * @return instance of org.frame.hadoop.sql.hbase.model.Page
	 */
	@SuppressWarnings("unchecked")
	public Page pagination(Class<?> clazz, Page page) {
		page = this.pagination(page);
		
		List<Object> data = this.mapList2ObjectList((List<Map<String, String>>) page.getData(), clazz);
		page.setData(data);
		
		return page;
	}
    
    public List<Map<String, String>> select(String name) {
    	List<Map<String, String>> result = new ArrayList<Map<String, String>>();
    	
    	Scan scan = new Scan();
    	ResultScanner rs = null;
    	HBaseAdmin admin = null;
    	HTable table = null;
    	try {
    		scan.setBatch(batch);
    		scan.setCaching(caching);
    		
    		admin = new HBaseAdmin(conn);
    		//if(admin.tableExists(name)){
    			table = new HTable(TableName.valueOf(name), conn);

    			rs = table.getScanner(scan);
    			result = this.rs2MapList(rs);
    		//}
    	} catch(IOException e) {
    		e.printStackTrace();
    	} finally {
    		this.destory(admin, table, rs);
    	}
    	
    	return result;
    }
    
    public List<Map<String, String>> select(String name, String family, String qualifier, String value, CompareOp operate) {
    	Map<String, Object> map = new HashMap<String, Object>();
    	map.put(HBase.HBASE_FAMILY, family);
    	map.put(HBase.HBASE_QUALIFIER, qualifier);
    	map.put(HBase.HBASE_VALUE, value);
    	map.put(HBase.HBASE_OPERATE, operate);
    	
    	List<Map<String, Object>> parameters = new ArrayList<Map<String, Object>>();
    	parameters.add(map);
    	
    	return this.select(name, parameters);
    }
    
    public List<Object> select(String name, String family, String qualifier, String value, CompareOp operate, Class<?> clazz) {
    	Map<String, Object> map = new HashMap<String, Object>();
    	map.put(HBase.HBASE_FAMILY, family);
    	map.put(HBase.HBASE_QUALIFIER, qualifier);
    	map.put(HBase.HBASE_VALUE, value);
    	map.put(HBase.HBASE_OPERATE, operate);
    	
    	List<Map<String, Object>> parameters = new ArrayList<Map<String, Object>>();
    	parameters.add(map);
    	
    	return this.select(name, parameters, clazz);
    }
    
    public List<Map<String, String>> select(String name, List<Map<String, Object>> parameters) {
    	List<Map<String, String>> result = new ArrayList<Map<String, String>>();

    	Filter filter;
    	List<Filter> filters = new ArrayList<Filter>();
    	for (Map<String, Object> map : parameters) {
    		filter = new SingleColumnValueFilter(Bytes.toBytes(String.valueOf(map.get(HBase.HBASE_FAMILY))), Bytes.toBytes(String.valueOf(map.get(HBase.HBASE_QUALIFIER))), (CompareOp) map.get(HBASE_OPERATE), Bytes.toBytes(String.valueOf(map.get(HBase.HBASE_VALUE))));
    		filters.add(filter);
    	}

    	FilterList filterList = new FilterList(filters);

    	Scan scan = new Scan();
    	ResultScanner rs = null;
    	HTable table = null;
    	try {
    		scan.setBatch(batch);
    		scan.setCaching(caching);
    		scan.setFilter(filterList);
    		
    		table = new HTable(TableName.valueOf(name), conn);

    		rs = table.getScanner(scan);
    		result = this.rs2MapList(rs);
    	} catch(IOException e) {
    		e.printStackTrace();
    	} finally {
    		this.destory(null, table, rs);
    	}

    	return result;
    }
    
    public List<Object> select(String name, List<Map<String, Object>> parameters, Class<?> clazz) {
    	return this.mapList2ObjectList(this.select(name, parameters), clazz);
    }
    
    public List<Object> select(String name, Class<?> clazz) {
    	List<Object> result = new ArrayList<Object>();

    	Scan scan = new Scan();
    	ResultScanner rs = null;
    	HTable table = null;
    	try {
    		scan.setBatch(batch);
    		scan.setCaching(caching);
    		
    		table = new HTable(TableName.valueOf(name), conn);

    		rs = table.getScanner(scan);
    		result = this.rs2ObjectList(rs, clazz);
    	} catch(IOException e) {
    		e.printStackTrace();
    	} finally {
    		this.destory(null, table, rs);
    	}

    	return result;
    }
    
    public void setBatch(int batch) {
		this.batch = batch;
	}
    
    public void setCaching(int caching) {
		this.caching = caching;
	}
    
    public boolean turncate(String name, String key) {
    	boolean result = false;

    	HTable table = null;
    	try {
    		table = new HTable(TableName.valueOf(name), conn);
    		table.setAutoFlush(false, true);
    		//table.setWriteBufferSize(2048);

    		HColumnDescriptor[] columns = table.getTableDescriptor().getColumnFamilies();
    		for (HColumnDescriptor column : columns) {
    			Delete delete = new Delete(Bytes.toBytes(key));
    			delete.deleteColumns(Bytes.toBytes(column.getNameAsString()), Bytes.toBytes(column.getNameAsString()));
    			table.delete(delete);
    		}

    		table.flushCommits();

    		result = true;
    	} catch (MasterNotRunningException e) {
    		e.printStackTrace();
    	} catch (ZooKeeperConnectionException e) {
    		e.printStackTrace();
    	} catch (IOException e) {
    		e.printStackTrace();
    	} finally {
    		this.destory(null, table, null);
    	}

    	return result;
    }
    
    public boolean update(String name, String key, String family, String qualifier, String value) {
    	boolean result = false;

    	HTable table = null;
    	try {
    		table = new HTable(TableName.valueOf(name), conn);
    		Put put = new Put(Bytes.toBytes(key));
    		put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
    		table.put(put);

    		result = true;
    	} catch (MasterNotRunningException e) {
    		e.printStackTrace();
    	} catch (ZooKeeperConnectionException e) {
    		e.printStackTrace();
    	} catch (IOException e) {
    		e.printStackTrace();
    	} finally {
    		this.destory(null, table, null);
    	}

    	return result;
    }
    
    public boolean update(String name, List<Map<String, String>> parameters) {
    	boolean result = false;

    	HTable table = null;
    	try {
    		table = new HTable(TableName.valueOf(name), conn);
    		table.setAutoFlush(false, true);
    		//table.setWriteBufferSize(2048);

    		Put put;
    		for (Map<String, String> map : parameters) {
    			put = new Put(Bytes.toBytes(map.get(HBase.HBASE_ROWKEY)));
    			put.add(Bytes.toBytes(map.get(HBase.HBASE_FAMILY)), Bytes.toBytes(map.get(HBase.HBASE_QUALIFIER)), Bytes.toBytes(map.get(HBase.HBASE_VALUE)));
    			table.put(put);
    		}

    		table.flushCommits();

    		result = true;
    	} catch (MasterNotRunningException e) {
    		e.printStackTrace();
    	} catch (ZooKeeperConnectionException e) {
    		e.printStackTrace();
    	} catch (IOException e) {
    		e.printStackTrace();
    	} finally {
    		this.destory(null, table, null);
    	}

    	return result;
    }
    
    public boolean update(Object object) {
    	Annotation annotation = new Annotation();
    	String name = annotation.table(object.getClass());
    	List<Map<String, String>> parameters = annotation.parameters(object);
    	
    	return this.update(name, parameters);
    }
    
    public boolean update(Object[] parameters) {
    	boolean result = true;
    	
    	for (Object object : parameters) {
    		result = result && this.update(object);
    	}
    	
    	return result;
    }
    
    public boolean update(List<Object> parameters) {
    	return this.update(new Object[parameters.size()]);
    }
    
    protected void destory(HBaseAdmin admin, HTable table, ResultScanner rs) {
    	if (rs != null) rs.close();
    	
    	try {
			if (table != null) table.close();
		} catch (IOException e) {
			admin = null;
			e.printStackTrace();
		}
		
		try {
			if (admin != null) admin.close();
		} catch (IOException e) {
			admin = null;
			e.printStackTrace();
		}
    }
    
    /**
	 * refresh instance of org.frame.model.system.page.Page
	 * 
	 * @param page instance of org.frame.model.system.page.Page to be refreshed
	 * @param sql sql for operation
	 * @param parameters parameters parameters for sql
	 * 
	 * @return instance of org.frame.model.system.page.Page be refreshed
	 */
    protected Page initPage(String table, List<Map<String, Object>> parameters, Page page) {
    	if (page == null) {
    		page = new Page(table, parameters);
    	} else {
    		page.setTable(table);
    		page.setParameters(parameters);
    	}
    	
    	return page;
    }
    
    /**
	 * reset instance of org.frame.model.system.page.Page
	 * 
	 * @param page instance of org.frame.model.system.page.Page to be reseted
	 * @param sql sql for operation
	 * @param parameters parameters parameters for sql
	 * 
	 * @return instance of org.frame.model.system.page.Page be reseted
	 */
	protected Page resetPage(Page page) {
		page.reset();
		return page;
	}
    
    protected Object map2Object(Map<String, String> map, Class<?> clazz) {
    	Object result = null;
    	
    	try {
    		if (map != null) {
        		Annotation annotation = new Annotation();
    			Reflect reflect = new Reflect();
    			
    			boolean isSimple = annotation.simple(clazz);

    			result = clazz.newInstance();
    			
    			Object data;
    			String key;
    			if (isSimple) {
    				for (Iterator<?> iterator = map.keySet().iterator(); iterator.hasNext();) {
						key = (String) iterator.next();
						reflect.set(result, key.toLowerCase(), map.get(key.toLowerCase()));
					}
    			} else {
    				Field[] fields = clazz.getDeclaredFields();
					for (Field field : fields) {
						if (field.isAnnotationPresent(Column.class)) {
							Column column = field.getAnnotation(Column.class);

							data = map.get(column.name().toLowerCase());
							reflect.set(result, field.getName().toLowerCase(), data);
						}
						
						if (field.isAnnotationPresent(Mirror.class)) {
							Mirror mirror = field.getAnnotation(Mirror.class);

							data = map.get(mirror.name().toLowerCase());
							reflect.set(result, field.getName().toLowerCase(), data);
						}
					}
    			}
        	}
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
    	
    	return result;
    }
    
    protected List<Object> mapList2ObjectList(List<Map<String, String>> list, Class<?> clazz) {
    	List<Object> result = null;
    	
    	if (list != null) {
    		result = new ArrayList<Object>();
    		for (Map<String, String> map : list) {
    			result.add(this.map2Object(map, clazz));
    		}
    	}
    	
    	return result;
    }
    
    protected Map<String, String> rs2Map(Result r) {
    	Map<String, String> result = null;
    	
    	if (r != null) {
    		result = new HashMap<String, String>();
        	for (Cell cell : r.rawCells()) {
        		result.put(new String(CellUtil.cloneFamily(cell)), new String(CellUtil.cloneValue(cell)));
        	}
    	}
    	
    	return result;
    }
    
    protected List<Map<String, String>> rs2MapList(ResultScanner rs) {
    	List<Map<String, String>> result = null;
    	
    	if (rs != null) {
    		result = new ArrayList<Map<String, String>>();
    		for (Result r : rs) {
    			result.add(this.rs2Map(r));
    		}
    	}
    	
    	return result;
    }
    
    protected Object rs2Object(Result r, Class<?> clazz) {
    	Object object = null;

		if (r != null) {
			try {
				boolean flag = false;
				Class<?>[] interfaces = clazz.getInterfaces();
				for (int i = 0; i < interfaces.length; i++) {
					if ("org.frame.sql.model.ResultSetWrapper".equals(interfaces[i].getName())) {
						flag = true;
						break;
					}
				}

				if (flag) {
					object = clazz.newInstance();
					try {
						object = clazz.getMethod("wrapper", Result.class).invoke(object, r);
					} catch (IllegalArgumentException e) {
						e.printStackTrace();
					} catch (InvocationTargetException e) {
						e.printStackTrace();
					} catch (NoSuchMethodException e) {
						e.printStackTrace();
					} catch (SecurityException e) {
						e.printStackTrace();
					}
				} else {
					Annotation annotation = new Annotation();
					Reflect reflect = new Reflect();

					boolean isSimple = annotation.simple(clazz);

					object = clazz.newInstance();

					if (isSimple) {
						for (Cell cell : r.rawCells()) {
							reflect.set(object, new String(CellUtil.cloneFamily(cell)).toLowerCase(), new String(CellUtil.cloneValue(cell)));
			        	}
					} else {
						object = this.map2Object(this.rs2Map(r), clazz);
					}
				}
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		}

    	return object;
    }
    
    protected List<Object> rs2ObjectList(ResultScanner rs, Class<?> clazz) {
    	List<Object> result = null;
    	
    	if (rs != null) {
    		result = new ArrayList<Object>();
    		for (Result r : rs) {
    			result.add(this.rs2Object(r, clazz));
    		}
    	}
    	
    	return result;
    }
 
    /*public static void main(String[] args) {
		HBase hBase = new HBase();
		
		List<Map<String, String>> list;
		
		String key, value;
		list = hBase.select("hbase_table_test");
		for (Map<String, String> map : list) {
			for (Iterator<?> iterator = map.keySet().iterator(); iterator.hasNext();) {
				key = (String) iterator.next();
				value = map.get(key.toLowerCase());
				
				System.out.println("key: " + key);
				System.out.println("value: " + value);
			}
		}
		
		System.out.println(new Date().toString());
		
		//list = hBase.select("hbase_table_test", "create_time", "ctime", "12:00:00");
		list = hBase.select("hbase_table_test");
		for (Map<String, String> map : list) {
			for (Iterator<?> iterator = map.keySet().iterator(); iterator.hasNext();) {
				key = (String) iterator.next();
				value = map.get(key.toLowerCase());
				
				System.out.println("key: " + key);
				System.out.println("value: " + value);
			}
		}
		
		System.out.println(new Date().toString());
		
		boolean flag = false;
		
		List<Map<String, String>> parameters = new ArrayList<Map<String, String>>();
		Map<String, String> map1 = new HashMap<String, String>();
		map1.put(HBase.HBASE_ROWKEY, "1");
		map1.put(HBase.HBASE_FAMILY, "create_time");
		map1.put(HBase.HBASE_QUALIFIER, "ctime");
		map1.put(HBase.HBASE_VALUE, "12:00:00");
		
		
		Map<String, String> map2 = new HashMap<String, String>();
		map2.put(HBase.HBASE_ROWKEY, "1");
		map2.put(HBase.HBASE_FAMILY, "info");
		map2.put(HBase.HBASE_QUALIFIER, "inf");
		map2.put(HBase.HBASE_VALUE, "aaaaa");
		
		parameters.add(map1);
		parameters.add(map2);
		
		System.out.println(new Date().toString());
		
		flag = hBase.insert("hbase_table_test", parameters);
		
		System.out.println(new Date().toString());
		
		//flag = hBase.insert("1", "hbase_table_test", "create_time", "ctime", "13:00:00");
		
		list = hBase.select("hbase_table_test");
		for (Map<String, String> map : list) {
			for (Iterator<?> iterator = map.keySet().iterator(); iterator.hasNext();) {
				key = (String) iterator.next();
				value = map.get(key.toLowerCase());
				
				System.out.println("key: " + key);
				System.out.println("value: " + value);
			}
		}
		
		flag = hBase.delete("hbase_table_test", "1");
		
		System.out.println(list.size());
	}*/
    
}
