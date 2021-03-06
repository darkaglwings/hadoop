package org.frame.hadoop.annotation.repository;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface Mirror {
	
	public static boolean TRUE = true;
	
	public static boolean FALSE = false;
	
	String desc() default "";
	
	String family() default "";
	
	String qualifier() default "";

	String type() default "";
	
	boolean primary() default false;
	
	boolean nullable() default true;
	
	int length() default 4000;
	
	int precision() default 0;
	
	int scale() default 0;
	
}
