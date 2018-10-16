package cn.edu360.hdfs.datacollect;

import java.util.Properties;

/**单例模式：懒汉式——考虑了线程安全*/
public class PropertyHolderLazy {
	private static Properties prop = null;
	public static Properties getProps() throws Exception {
		if (prop == null) {	//	防止每次都创建一个Properties，只有prop == null时，才创建一个Properties；
			// 不能锁整个方法，锁整个方法，各个线程要等待获取锁才能执行获得prop对象；只锁创建prop对象部分，当prop != null，不涉及锁的问题，效率更高
			synchronized (PropertyHolderLazy.class) {
				if (prop == null) {
					prop = new Properties();
					prop.load(PropertyHolderLazy.class.getClassLoader().getResourceAsStream("collect.properties"));
				}
			}
		}
		return prop;
	}
}
