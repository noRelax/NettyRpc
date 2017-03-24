package com.nettyrpc.protocol;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.objenesis.Objenesis;
import org.objenesis.ObjenesisStd;

import com.dyuproject.protostuff.LinkedBuffer;
import com.dyuproject.protostuff.ProtostuffIOUtil;
import com.dyuproject.protostuff.Schema;
import com.dyuproject.protostuff.runtime.RuntimeSchema;

/**
 * Serialization Util（Based on Protostuff）
 * 
 * @author huangyong objenesis（反序列化工具类）简介：
 * 
 *         objenesis是一个小型Java类库用来实例化一个特定class的对象。
 * 
 *         使用场合：
 * 
 *         Java已经支持使用Class.newInstance()动态实例化类的实例。但是类必须拥有一个合适的构造器。
 *         有很多场景下不能使用这种方式实例化类，比如：
 * 
 *         构造器需要参数
 * 
 *         构造器有side effects
 * 
 *         构造器会抛异常
 * 
 *         因此，在类库中经常会有类必须拥有一个默认构造器的限制。Objenesis通过绕开对象实例构造器来克服这个限制。
 * 
 *         protobuf（序列化工具类）
 * 
 *         Google 的protobuf是一个优秀的序列化工具，跨语言、快速、序列化后体积小。
 *         protobuf的一个缺点是需要数据结构的预编译过程，首先要编写.proto格式的配置文件，
 *         再通过protobuf提供的工具生成各种语言响应的代码。由于java具有反射和动态代码生成的能力，这个预编译过程不是必须的，
 *         可以在代码执行时来实现。有个protostuff(http://code.google.com/p/protostuff/)
 *         已经实现了这个功能。
 * 
 *         protostuff基于Google
 *         protobuf，但是提供了更多的功能和更简易的用法。其中，protostuff-runtime实现了无需预编译对java
 *         bean进行protobuf序列化/反序列化的能力。
 * 
 *         protostuff-runtime的局限是序列化前需预先传入schema，反序列化不负责对象的创建只负责复制，因而必须提供默认构造函数。
 * 
 *         此外，protostuff还可以按照protobuf的配置序列化成json/yaml/xml等格式。
 */
public class SerializationUtil {

	private static Map<Class<?>, Schema<?>> cachedSchema = new ConcurrentHashMap<>();

	private static Objenesis objenesis = new ObjenesisStd(true);

	private SerializationUtil() {
	}

	@SuppressWarnings("unchecked")
	private static <T> Schema<T> getSchema(Class<T> cls) {
		Schema<T> schema = (Schema<T>) cachedSchema.get(cls);
		if (schema == null) {
			schema = RuntimeSchema.createFrom(cls);
			if (schema != null) {
				cachedSchema.put(cls, schema);
			}
		}
		return schema;
	}

	/**
	 * 序列化（对象 -> 字节数组）
	 */
	@SuppressWarnings("unchecked")
	public static <T> byte[] serialize(T obj) {
		Class<T> cls = (Class<T>) obj.getClass();
		LinkedBuffer buffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);
		try {
			Schema<T> schema = getSchema(cls);
			return ProtostuffIOUtil.toByteArray(obj, schema, buffer);
		} catch (Exception e) {
			throw new IllegalStateException(e.getMessage(), e);
		} finally {
			buffer.clear();
		}
	}

	/**
	 * 反序列化（字节数组 -> 对象）
	 */
	public static <T> T deserialize(byte[] data, Class<T> cls) {
		try {
			T message = (T) objenesis.newInstance(cls);
			Schema<T> schema = getSchema(cls);
			ProtostuffIOUtil.mergeFrom(data, message, schema);
			return message;
		} catch (Exception e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}
}
