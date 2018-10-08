package com.epam.bigdata.UserAgentDetail;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import eu.bitwalker.useragentutils.UserAgent;

/**
 * To parse the userAgent data from and get the properties like ostype, device,
 * browser.
 * 
 * @author Shiva_Donkena
 *
 */
public class UserAgentUDF extends GenericUDTF {

	private enum Fields {
		device, os_name, browser, ua
	};

	private PrimitiveObjectInspector stringOI = null;

	@Override
	public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
		if (args.length != 1) {
			throw new UDFArgumentException("pass only 1 argument");
		}

		if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE || ((PrimitiveObjectInspector) args[0])
				.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
			throw new UDFArgumentException(" Pass the string as a parameter");
		}

		stringOI = (PrimitiveObjectInspector) args[0];

		ArrayList<String> fieldNames = new ArrayList<String>(Fields.values().length);
		ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(Fields.values().length);

		for (Fields field : Fields.values()) {
			fieldNames.add(field.toString());
			fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		}
		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
	}

	/**
	 * To read the each property of the userAgent adding to an object.
	 */
	@Override
	public void process(Object[] args) throws HiveException {
		final String userAgentString = stringOI.getPrimitiveJavaObject(args[0]).toString();

		if (userAgentString == null || userAgentString.isEmpty()) {
			return;
		}
		forward(createParseDataObject(UserAgent.parseUserAgentString(userAgentString)));
	}

	public static Object[] createParseDataObject(UserAgent userAgent) {
		Object[] object = new Object[] { userAgent.getOperatingSystem().getDeviceType().getName(),
				userAgent.getOperatingSystem().getName(), userAgent.getBrowser().getName(),
				userAgent.getBrowser().getBrowserType().getName() };
		return object;
	}

	@Override
	public void close() {

	}
}
