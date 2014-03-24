/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2012 Pentaho
// Copyright (c) 2008-2013 Open Link Financial, Inc. All Rights Reserved.
 */
package custom.mondrian.xmla.handler;

import mondrian.olap.CacheControl;
import mondrian.olap.CacheControl.CellRegion;
import mondrian.olap.MondrianException;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Util;
import custom.mondrian.properties.ExternalProperties;
import custom.mondrian.xmla.exception.XmlaException;
import custom.mondrian.xmla.handler.Enumeration.ResponseMimeType;
import custom.mondrian.xmla.request.XmlaRequest;
import custom.mondrian.xmla.request.impl.DefaultXmlaRequest;
import custom.mondrian.xmla.response.XmlaResponse;
import custom.mondrian.xmla.response.impl.DefaultXmlaResponse;
import custom.mondrian.xmla.writer.DefaultSaxWriter;
import custom.mondrian.xmla.writer.SaxWriter;
import org.apache.log4j.Logger;
import org.olap4j.*;
import org.olap4j.impl.Olap4jUtil;
import org.olap4j.metadata.*;
import org.olap4j.metadata.Property.StandardCellProperty;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.*;
import java.util.Date;
import mondrian.rolap.RolapConnection;
import mondrian.server.Statement;
import static custom.mondrian.xmla.handler.XmlaConstants.*;
import static org.olap4j.metadata.XmlaConstants.*;

/**
 * An <code>XmlaHandler</code> responds to XML for Analysis (XML/A) requests.
 * 
 * @author jhyde, Gang Chen
 * @since 27 April, 2003
 */
public class CustomXmlaHandler extends mondrian.xmla.XmlaHandler {

   private CacheControl factCacheControl;
   private static final Logger LOGGER = Logger.getLogger(CustomXmlaHandler.class);

   /**
    * Name of property used by JDBC to hold user name.
    */
   static final String JDBC_USER = "user";
   /**
    * Name of property used by JDBC to hold password.
    */
   static final String JDBC_PASSWORD = "password";
   
   static final String MD_DATA_SET_XML_SCHEMA = computeXsd(SetType.MD_DATA_SET);

   static final String NS_XML_SQL = "urn:schemas-microsoft-com:xml-sql";

   //
   // Some xml schema data types.
   //
   public static final String XSD_BOOLEAN = "xs:boolean";
   public static final String XSD_STRING = "xs:string";
   public static final String XSD_UNSIGNED_INT = "xs:unsignedInt";

   public static final String XSD_BYTE = "xs:byte";
   public static final byte XSD_BYTE_MAX_INCLUSIVE = 127;
   public static final byte XSD_BYTE_MIN_INCLUSIVE = -128;

   public static final String XSD_SHORT = "xs:short";
   public static final short XSD_SHORT_MAX_INCLUSIVE = 32767;
   public static final short XSD_SHORT_MIN_INCLUSIVE = -32768;

   public static final String XSD_INT = "xs:int";
   public static final int XSD_INT_MAX_INCLUSIVE = 2147483647;
   public static final int XSD_INT_MIN_INCLUSIVE = -2147483648;

   public static final String XSD_LONG = "xs:long";
   public static final long XSD_LONG_MAX_INCLUSIVE = 9223372036854775807L;
   public static final long XSD_LONG_MIN_INCLUSIVE = -9223372036854775808L;

   // xsd:double: IEEE 64-bit floating-point
   public static final String XSD_DOUBLE = "xsd:double";

   // xsd:decimal: Decimal numbers (BigDecimal)
   public static final String XSD_DECIMAL = "xsd:decimal";

   // xsd:integer: Signed integers of arbitrary length (BigInteger)
   public static final String XSD_INTEGER = "xsd:integer";

    /**
     * Name of property used by JDBC to hold locale. It is not hard-wired into
     * DriverManager like "user" and "password", but we do expect any olap4j
     * driver that supports i18n to use this property name.
     */
   public static final String JDBC_LOCALE = "locale";
   
   static String lastMdx = "";
   static String currentCube = "";

   public final ConnectionFactory connectionFactory;
   


   public static XmlaExtra getExtra(OlapConnection connection) {
      try {
         final XmlaExtra extra = connection.unwrap(XmlaExtra.class);
         if (extra != null) {
            return extra;
         }
      } catch (SQLException e) {
         // Connection cannot provide an XmlaExtra. Fall back and give a
         // default implementation.
      } catch (UndeclaredThrowableException ute) {
         //
         // Note: this is necessary because we use a dynamic proxy for the
         // connection.
         // I could not catch and un-wrap the Undeclared Throwable within
         // the proxy.
         // The exception comes out here and I couldn't find any better
         // ways to deal with it.
         //
         // The undeclared throwable contains an Invocation Target Exception
         // which in turns contains the real exception thrown by the "unwrap"
         // method, for example OlapException.
         //

         Throwable cause = ute.getCause();
         if (cause instanceof InvocationTargetException) {
            cause = cause.getCause();
         }

         // this maintains the original behaviour: don't catch exceptions
         // that are not subclasses of SQLException

         if (!(cause instanceof SQLException)) {
            throw ute;
         }
      }
      return new XmlaExtraImpl();
   }

   /**
    * Returns a new OlapConnection opened with the credentials specified in the
    * XMLA request or an existing connection if one can be found associated with
    * the request session id.
    * 
    * @param request
    *           Request
    * @param propMap
    *           Extra properties
    */

   @SuppressWarnings("unchecked")
   public OlapConnection getConnection(XmlaRequest request, Map<String, String> propMap) {
      String sessionId = request.getSessionId();
      if (sessionId == null) {
         sessionId = "<no_session>";
      }
      LOGGER.debug("Creating new connection for user [" + request.getUsername() + "] and session [" + sessionId + "]");

      Properties props = new Properties();
      for (Map.Entry<String, String> entry : propMap.entrySet()) {
         props.put(entry.getKey(), entry.getValue());
      }
      if (request.getUsername() != null) {
         props.put(JDBC_USER, request.getUsername());
      }
      if (request.getPassword() != null) {
         props.put(JDBC_PASSWORD, request.getPassword());
      }

      final String databaseName = (String) request.getProperties().get(PropertyDefinition.DataSourceInfo.name());

      String catalogName = (String) request.getProperties().get(PropertyDefinition.Catalog.name());

      if (catalogName == null && request.getMethod() == Method.DISCOVER && request.getRestrictions().containsKey(Property.StandardMemberProperty.CATALOG_NAME.name())) {
         Object restriction = request.getRestrictions().get(Property.StandardMemberProperty.CATALOG_NAME.name());
         if (restriction instanceof List) {

            final List<Object> requiredValues = (List<Object>) restriction;
            catalogName = String.valueOf(requiredValues.get(0));
         } else {
            throw Util.newInternal("unexpected restriction type: " + restriction.getClass());
         }
      }

      return getConnection(databaseName, catalogName, request.getRoleName(), props);
   }

   private enum SetType {
      ROW_SET, MD_DATA_SET
   }



   public static boolean isValidXsdInt(long l) {
      return (l <= XSD_INT_MAX_INCLUSIVE) && (l >= XSD_INT_MIN_INCLUSIVE);
   }

   /**
    * Takes a DataType String (null, Integer, Numeric or non-null) and Value
    * Object (Integer, Double, String, other) and canonicalizes them to XSD data
    * type and corresponding object.
    * <p>
    * If the input DataType is Integer, then it attempts to return an XSD_INT
    * with value java.lang.Integer (and failing that an XSD_LONG
    * (java.lang.Long) or XSD_INTEGER (java.math.BigInteger)). Worst case is the
    * value loses precision with any integral representation and must be
    * returned as a decimal type (Double or java.math.BigDecimal).
    * <p>
    * If the input DataType is Decimal, then it attempts to return an XSD_DOUBLE
    * with value java.lang.Double (and failing that an XSD_DECIMAL
    * (java.math.BigDecimal)).
    */
   static class ValueInfo {

      /**
       * Returns XSD_INT, XSD_DOUBLE, XSD_STRING or null.
       * 
       * @param dataType
       *           null, Integer, Numeric or non-null.
       * @return Returns the suggested XSD type for a given datatype
       */
      static String getValueTypeHint(final String dataType) {
         if (dataType != null) {
            return (dataType.equals("Integer")) ? XSD_INT : ((dataType.equals("Numeric")) ? XSD_DECIMAL : XSD_STRING);
         } else {
            return null;
         }
      }

      String valueType;
      Object value;
      boolean isDecimal;

      ValueInfo(final String dataType, final Object inputValue) {
         final String valueTypeHint = getValueTypeHint(dataType);

         // This is a hint: should it be a string, integer or decimal type.
         // In the following, if the hint is integer, then there is
         // an attempt that the value types
         // be XSD_INT, XST_LONG, or XSD_INTEGER (but they could turn
         // out to be XSD_DOUBLE or XSD_DECIMAL if precision is loss
         // with the integral formats). It the hint is a decimal type
         // (double, float, decimal), then a XSD_DOUBLE or XSD_DECIMAL
         // is returned.
         if (valueTypeHint != null) {
            // The value type is a hint. If the value can be
            // converted to the data type without precision loss, ok;
            // otherwise value data type must be adjusted.

            if (valueTypeHint.equals(XSD_STRING)) {
               // For String types, nothing to do.
               this.valueType = valueTypeHint;
               this.value = inputValue;
               this.isDecimal = false;

            } else if (valueTypeHint.equals(XSD_INT)) {
               // If valueTypeHint is XSD_INT, then see if value can be
               // converted to (first choice) integer, (second choice),
               // long and (last choice) BigInteger - otherwise must
               // use double/decimal.

               // Most of the time value ought to be an Integer so
               // try it first
               if (inputValue instanceof Integer) {
                  // For integer, its already the right type
                  this.valueType = valueTypeHint;
                  this.value = inputValue;
                  this.isDecimal = false;

               } else if (inputValue instanceof Byte) {
                  this.valueType = valueTypeHint;
                  this.value = inputValue;
                  this.isDecimal = false;

               } else if (inputValue instanceof Short) {
                  this.valueType = valueTypeHint;
                  this.value = inputValue;
                  this.isDecimal = false;

               } else if (inputValue instanceof Long) {
                  // See if it can be an integer or long
                  long lval = (Long) inputValue;
                  setValueAndType(lval);

               } else if (inputValue instanceof BigInteger) {
                  BigInteger bi = (BigInteger) inputValue;
                  // See if it can be an integer or long
                  long lval = bi.longValue();
                  if (bi.equals(BigInteger.valueOf(lval))) {
                     // It can be converted from BigInteger to long
                     // without loss of precision.
                     setValueAndType(lval);
                  } else {
                     // It can not be converted to a long.
                     this.valueType = XSD_INTEGER;
                     this.value = inputValue;
                     this.isDecimal = false;
                  }

               } else if (inputValue instanceof Float) {
                  Float f = (Float) inputValue;
                  // See if it can be an integer or long
                  long lval = f.longValue();
                  if (f.equals(new Float(lval))) {
                     // It can be converted from double to long
                     // without loss of precision.
                     setValueAndType(lval);

                  } else {
                     // It can not be converted to a long.
                     this.valueType = XSD_DOUBLE;
                     this.value = inputValue;
                     this.isDecimal = true;
                  }

               } else if (inputValue instanceof Double) {
                  Double d = (Double) inputValue;
                  // See if it can be an integer or long
                  long lval = d.longValue();
                  if (d.equals(new Double(lval))) {
                     // It can be converted from double to long
                     // without loss of precision.
                     setValueAndType(lval);

                  } else {
                     // It can not be converted to a long.
                     this.valueType = XSD_DOUBLE;
                     this.value = inputValue;
                     this.isDecimal = true;
                  }

               } else if (inputValue instanceof BigDecimal) {
                  // See if it can be an integer or long
                  BigDecimal bd = (BigDecimal) inputValue;
                  try {
                     // Can it be converted to a long
                     // Throws ArithmeticException on conversion failure.
                     // The following line is only available in
                     // Java5 and above:
                     // long lval = bd.longValueExact();
                     long lval = bd.longValue();

                     setValueAndType(lval);
                  } catch (ArithmeticException ex) {
                     // No, it can not be converted to long

                     try {
                        // Can it be an integer
                        BigInteger bi = bd.toBigIntegerExact();
                        this.valueType = XSD_INTEGER;
                        this.value = bi;
                        this.isDecimal = false;
                     } catch (ArithmeticException ex1) {
                        // OK, its a decimal
                        this.valueType = XSD_DECIMAL;
                        this.value = inputValue;
                        this.isDecimal = true;
                     }
                  }

               } else if (inputValue instanceof Number) {
                  // Don't know what Number type we have here.
                  // Note: this could result in precision loss.
                  this.value = ((Number) inputValue).longValue();
                  this.valueType = valueTypeHint;
                  this.isDecimal = false;

               } else {
                  // Who knows what we are dealing with,
                  // hope for the best?!?
                  this.valueType = valueTypeHint;
                  this.value = inputValue;
                  this.isDecimal = false;
               }

            } else if (valueTypeHint.equals(XSD_DOUBLE) || valueTypeHint.equals(XSD_DECIMAL)) {
               // The desired type is double.

               // Most of the time value ought to be an Double so
               // try it first
               if (inputValue instanceof Double) {
                  // For Double, its already the right type
                  this.valueType = valueTypeHint;
                  this.value = inputValue;
                  this.isDecimal = true;

               } else if (inputValue instanceof Byte || inputValue instanceof Short || inputValue instanceof Integer || inputValue instanceof Long) {
                  // Convert from byte/short/integer/long to double
                  this.value = ((Number) inputValue).doubleValue();
                  this.valueType = valueTypeHint;
                  this.isDecimal = true;

               } else if (inputValue instanceof Float) {
                  this.value = inputValue;
                  this.valueType = valueTypeHint;
                  this.isDecimal = true;

               } else if (inputValue instanceof BigDecimal) {
                  BigDecimal bd = (BigDecimal) inputValue;
                  double dval = bd.doubleValue();
                  // make with same scale as Double
                  try {
                     BigDecimal bd2 = Util.makeBigDecimalFromDouble(dval);
                     // Can it be a double
                     // Must use compareTo - see BigDecimal.equals
                     if (bd.compareTo(bd2) == 0) {
                        this.valueType = XSD_DOUBLE;
                        this.value = dval;
                     } else {
                        this.valueType = XSD_DECIMAL;
                        this.value = inputValue;
                     }
                  } catch (NumberFormatException ex) {
                     this.valueType = XSD_DECIMAL;
                     this.value = inputValue;
                  }
                  this.isDecimal = true;

               } else if (inputValue instanceof BigInteger) {
                  // What should be done here? Convert ot BigDecimal
                  // and see if it can be a double or not?
                  // See if there is loss of precision in the convertion?
                  // Don't know. For now, just keep it a integral
                  // value.
                  BigInteger bi = (BigInteger) inputValue;
                  // See if it can be an integer or long
                  long lval = bi.longValue();
                  if (bi.equals(BigInteger.valueOf(lval))) {
                     // It can be converted from BigInteger to long
                     // without loss of precision.
                     setValueAndType(lval);
                  } else {
                     // It can not be converted to a long.
                     this.valueType = XSD_INTEGER;
                     this.value = inputValue;
                     this.isDecimal = true;
                  }

               } else if (inputValue instanceof Number) {
                  // Don't know what Number type we have here.
                  // Note: this could result in precision loss.
                  this.value = ((Number) inputValue).doubleValue();
                  this.valueType = valueTypeHint;
                  this.isDecimal = true;

               } else {
                  // Who knows what we are dealing with,
                  // hope for the best?!?
                  this.valueType = valueTypeHint;
                  this.value = inputValue;
                  this.isDecimal = true;
               }
            }
         } else {
            // There is no valueType "hint", so just get it from the value.
            if (inputValue instanceof String) {
               this.valueType = XSD_STRING;
               this.value = inputValue;
               this.isDecimal = false;

            } else if (inputValue instanceof Integer) {
               this.valueType = XSD_INT;
               this.value = inputValue;
               this.isDecimal = false;

            } else if (inputValue instanceof Byte) {
               Byte b = (Byte) inputValue;
               this.valueType = XSD_INT;
               this.value = b.intValue();
               this.isDecimal = false;

            } else if (inputValue instanceof Short) {
               Short s = (Short) inputValue;
               this.valueType = XSD_INT;
               this.value = s.intValue();
               this.isDecimal = false;

            } else if (inputValue instanceof Long) {
               // See if it can be an integer or long
               setValueAndType((Long) inputValue);

            } else if (inputValue instanceof BigInteger) {
               BigInteger bi = (BigInteger) inputValue;
               // See if it can be an integer or long
               long lval = bi.longValue();
               if (bi.equals(BigInteger.valueOf(lval))) {
                  // It can be converted from BigInteger to long
                  // without loss of precision.
                  setValueAndType(lval);
               } else {
                  // It can not be converted to a long.
                  this.valueType = XSD_INTEGER;
                  this.value = inputValue;
                  this.isDecimal = false;
               }

            } else if (inputValue instanceof Float) {
               this.valueType = XSD_DOUBLE;
               this.value = inputValue;
               this.isDecimal = true;

            } else if (inputValue instanceof Double) {
               this.valueType = XSD_DOUBLE;
               this.value = inputValue;
               this.isDecimal = true;

            } else if (inputValue instanceof BigDecimal) {
               // See if it can be a double
               BigDecimal bd = (BigDecimal) inputValue;
               double dval = bd.doubleValue();
               // make with same scale as Double
               try {
                  BigDecimal bd2 = Util.makeBigDecimalFromDouble(dval);
                  // Can it be a double
                  // Must use compareTo - see BigDecimal.equals
                  if (bd.compareTo(bd2) == 0) {
                     this.valueType = XSD_DOUBLE;
                     this.value = dval;
                  } else {
                     this.valueType = XSD_DECIMAL;
                     this.value = inputValue;
                  }
               } catch (NumberFormatException ex) {
                  this.valueType = XSD_DECIMAL;
                  this.value = inputValue;
               }
               this.isDecimal = true;

            } else if (inputValue instanceof Number) {
               // Don't know what Number type we have here.
               // Note: this could result in precision loss.
               this.value = ((Number) inputValue).longValue();
               this.valueType = XSD_LONG;
               this.isDecimal = false;

            } else {
               // Who knows what we are dealing with,
               // hope for the best?!?
               this.valueType = XSD_STRING;
               this.value = inputValue;
               this.isDecimal = false;
            }
         }
      }

      private void setValueAndType(long lval) {
         if (!isValidXsdInt(lval)) {
            // No, it can not be a integer, must be a long
            this.valueType = XSD_LONG;
            this.value = lval;
         } else {
            // Its an integer.
            this.valueType = XSD_INT;
            this.value = (int) lval;
         }
         this.isDecimal = false;
      }
   }

   private static String computeXsd(SetType setType) {
      final StringWriter sw = new StringWriter();
      SaxWriter writer = (SaxWriter) new DefaultSaxWriter(new PrintWriter(sw), 3);
      writeDatasetXmlSchema(writer, setType);
      writer.flush();
      return sw.toString();
   }

   /**
    * Creates an <code>XmlaHandler</code>.
    * 
    * <p>
    * The connection factory may be null, as long as you override
    * {@link #getConnection(String, String, String, Properties)}.
    * 
    * @param connectionFactory
    *           Connection factory
    * @param prefix
    *           XML Namespace. Typical value is "xmla", but a value of "cxmla"
    *           works around an Internet Explorer 7 bug
    */
   public CustomXmlaHandler(ConnectionFactory connectionFactory, String prefix) {
      super(connectionFactory, prefix);
      assert prefix != null;
      this.connectionFactory = connectionFactory;

      if (!prefix.equalsIgnoreCase("xmla")&& !prefix.equalsIgnoreCase("cxmla")) {
         throw new MondrianException("Only accept SOAPT message starts with 'xmla' namespace.");
      }
   }

   /**
    * Processes a request.
    * 
    * @param request
    *           XML request, for example, "<SOAP-ENV:Envelope ...>".
    * @param response
    *           Destination for response
    * @throws XmlaException
    *            on error
    */
   public void process(XmlaRequest request, XmlaResponse response) throws XmlaException {
      Method method = request.getMethod();
      long start = System.currentTimeMillis();

      switch (method) {
      case DISCOVER:
         discover(request, response);
         break;
      case EXECUTE:
         execute(request, response);
         break;
      default:
         throw new XmlaException(CLIENT_FAULT_FC, HSB_BAD_METHOD_CODE, HSB_BAD_METHOD_FAULT_FS, new IllegalArgumentException("Unsupported XML/A method: " + method));
      }
      if (LOGGER.isDebugEnabled()) {
         long end = System.currentTimeMillis();
         LOGGER.debug("XmlaHandler.process: time = " + (end - start));
         LOGGER.debug("XmlaHandler.process: " + Util.printMemory());
      }
   }

   private void checkFormat(XmlaRequest request) throws XmlaException {
      // Check response's rowset format in request
      final Map<String, String> properties = request.getProperties();
      if (request.isDrillThrough()) {
         Format format = getFormat(request, null);
         if (format != Format.Tabular) {
            throw new XmlaException(CLIENT_FAULT_FC, HSB_DRILL_THROUGH_FORMAT_CODE, HSB_DRILL_THROUGH_FORMAT_FAULT_FS, new UnsupportedOperationException(
                     "<Format>: only 'Tabular' allowed when drilling " + "through"));
         }
      } else {
         final String formatName = properties.get(PropertyDefinition.Format.name());
         if (formatName != null) {
            Format format = getFormat(request, null);
            if (format != Format.Multidimensional && format != Format.Tabular) {

               format = Format.Multidimensional;

            }
         }
         final String axisFormatName = properties.get(PropertyDefinition.AxisFormat.name());
         if (axisFormatName != null) {
            AxisFormat axisFormat = Util.lookup(AxisFormat.class, axisFormatName, null);

            if (axisFormat != AxisFormat.TupleFormat) {
               throw new UnsupportedOperationException("<AxisFormat>: only 'TupleFormat' currently supported");
            }
         }
      }
   }

   private void execute(XmlaRequest request, XmlaResponse response) throws XmlaException {
      final Map<String, String> properties = request.getProperties();

      // Default responseMimeType is SOAP.
      Enumeration.ResponseMimeType responseMimeType = getResponseMimeType(request);

      // Default value is SchemaData, or Data for JSON responses.
      final String contentName = properties.get(PropertyDefinition.Content.name());
      Content content = Util.lookup(Content.class, contentName, responseMimeType == Enumeration.ResponseMimeType.JSON ? Content.Data : Content.DEFAULT);

      // Handle execute
      QueryResult result = null;
      try {
         if (request.isDrillThrough()) {
            result = executeDrillThroughQuery(request);
         } else {
            result = executeQuery(request);
         }

         SaxWriter writer = response.getWriter();

         writer.startDocument();

         writer.startElement("ExecuteResponse", "xmlns", NS_XMLA);
         writer.startElement("return");
         boolean rowset = false;
         if (request.getProperties().containsKey("Format") && request.getProperties().get("Format").equals("Multidimensional")) {
            rowset = false;
         } else {
            rowset = request.isDrillThrough()
                     || (Format.Tabular.name().equals(request.getProperties().get(PropertyDefinition.Format.name())) || Format.Tabular.name().equalsIgnoreCase("TABULAR"));
         }
         writer.startElement("root", "xmlns",
                  result == null ? NS_XMLA_EMPTY : rowset ? NS_XMLA_ROWSET : NS_XMLA_MDDATASET,

                  "xmlns:xsi", NS_XSI, "xmlns:xsd", NS_XSD, "xmlns:msxmla", MS_XMLA);

         switch (content) {
         case Schema:
         case SchemaData:
            if (result != null) {
               result.metadata(writer);
            } else {
               /* default message for empty result */
            }
            break;
         }

         try {
            switch (content) {
            case Data:
            case SchemaData:
            case DataOmitDefaultSlicer:
            case DataIncludeDefaultSlicer:
               if (result != null) {
                  result.unparse(writer);
               }
               break;
            }
         } catch (XmlaException xex) {
            throw xex;
         } catch (Throwable t) {
            throw new XmlaException(SERVER_FAULT_FC, HSB_EXECUTE_UNPARSE_CODE, HSB_EXECUTE_UNPARSE_FAULT_FS, t);
         } finally {
            writer.endElement(); // root
            writer.endElement(); // return
            writer.endElement(); // ExecuteResponse
         }
         writer.endDocument();
      } finally {
         if (result != null) {
            try {
               result.close();
            } catch (SQLException e) {
               // ignore
            }
         }
      }
   }

   /**
    * Computes the XML Schema for a dataset.
    * 
    * @param writer
    *           SAX writer
    * @param settype
    *           rowset or dataset?
    * @see RowsetDefinition#writeRowsetXmlSchema(SaxWriter)
    */
   static void writeDatasetXmlSchema(SaxWriter writer, SetType settype) {
      // String setNsXmla =
      // (settype == SetType.ROW_SET)
      // ? NS_XMLA_ROWSET
      // : NS_XMLA_MDDATASET;
      String setNsXmla = NS_XMLA_MDDATASET;

      writer.startElement(
               // "xsd:schema",
               "xs:schema", "xmlns:xs", NS_XSD, "targetNamespace", setNsXmla, "xmlns", setNsXmla, "xmlns:xsi", NS_XSI, "xmlns:sql", NS_XML_SQL, "elementFormDefault", "qualified",
               "xmlns:msxmla", MS_XMLA);
      writer.element("xs:import", "namespace", "http://schemas.microsoft.com/analysisservices/2003/xmla");

      // MemberType

      writer.startElement(
      // "xsd:complexType",
               "xs:complexType", "name", "MemberType");
      writer.startElement("xs:sequence");

      writer.element(
      // "xsd:any",
               "xs:any", "namespace", "##targetNamespace", "minOccurs", "0", "maxOccurs", "unbounded", "processContents", "skip");
      writer.endElement(); // xs:sequence
      writer.element("xs:attribute", "name", "Hierarchy", "type", XSD_STRING);
      writer.endElement(); // xs:complexType name="MemberType"


      writer.startElement("xs:complexType", "name", "PropType");

      writer.startElement("xs:sequence");
      writer.element("xs:element", "name", "Default", "minOccurs", "0");
      writer.endElement(); // xs.sequence
      writer.element("xs:attribute", "name", "name", "type", XSD_STRING, "use", "required");
      writer.element("xs:attribute", "name", "type", "type", "xs:QName");

      writer.endElement(); // xsd:complexType name="PropType"

      // TupleType

      writer.startElement("xs:complexType", "name", "TupleType");
      writer.startElement("xs:sequence");
      writer.element("xs:element", "name", "Member", "type", "MemberType", "maxOccurs", "unbounded");
      writer.endElement(); // xsd:sequence
      writer.endElement(); // xsd:complexType name="TupleType"

      // MembersType

      writer.startElement("xs:complexType", "name", "MembersType");
      writer.startElement("xs:sequence");
      writer.element("xs:element", "name", "Member", "type", "MemberType", "minOccurs", "0", "maxOccurs", "unbounded");
      writer.endElement(); // xsd:sequence
      writer.element("xs:attribute", "name", "Hierarchy", "type", XSD_STRING, "use", "required");
      writer.endElement(); // xsd:complexType

      // TuplesType

      writer.startElement("xs:complexType", "name", "TuplesType");
      writer.startElement("xs:sequence");
      writer.element("xs:element", "name", "Tuple", "type", "TupleType", "minOccurs", "0", "maxOccurs", "unbounded");
      writer.endElement(); // xsd:sequence
      writer.endElement(); // xsd:complexType

      // SetType

      writer.startElement("xs:group", "name", "SetType");
      writer.startElement("xs:choice");
      writer.element("xs:element", "name", "Members", "type", "MembersType");
      writer.element("xs:element", "name", "Tuples", "type", "TuplesType");
      writer.element("xs:element", "name", "CrossProduct", "type", "SetListType");
      writer.element("xs:element", "ref", "msxmla:NormTupleSet");
      writer.startElement("xs:element", "name", "Union");
      writer.startElement("xs:complexType");
      writer.element("xs:group", "ref", "SetType", "minOccurs", "0", "maxOccurs", "ubbounded");
      writer.endElement(); // xs:complexType
      writer.endElement(); // xs:element
      writer.endElement(); // xs:choice
      writer.endElement(); // xs:group

      // SetListType
      writer.startElement("xs:complexType", "name", "SetListType");
      writer.element("xs:group", "ref", "SetType", "minOccurs", "0", "maxOccurs", "unbounded");
      writer.element("xs:attribute", "name", "Size", "type", "xs:unsignedInt");
      writer.endElement(); // xs:complexType

      // // CrossProductType
      //
      // writer.startElement(
      // "xs:complexType",
      // "name", "CrossProductType");
      // writer.startElement("xs:sequence");
      // writer.startElement(
      // "xs:choice",
      // "minOccurs", 0,
      // "maxOccurs", "unbounded");
      // writer.element(
      // "xs:element",
      // "name", "Members",
      // "type", "MembersType");
      // writer.element(
      // "xs:element",
      // "name", "Tuples",
      // "type", "TuplesType");
      // writer.endElement(); // xsd:choice
      // writer.endElement(); // xsd:sequence
      // // writer.element(
      // // "xs:attribute",
      // // "name", "Size",
      // // "type", XSD_UNSIGNED_INT);
      // writer.endElement(); // xsd:complexType

      // OlapInfo

      writer.startElement("xs:complexType", "name", "OlapInfo");
      writer.startElement("xs:sequence");

      { // <CubeInfo>
         writer.startElement("xs:element", "name", "CubeInfo");
         writer.startElement("xs:complexType");
         writer.startElement("xs:sequence");

         { // <Cube>
            writer.startElement("xs:element", "name", "Cube", "maxOccurs", "unbounded");
            writer.startElement("xs:complexType");
            writer.startElement("xs:sequence");

            writer.element("xs:element", "name", "CubeName", "type", XSD_STRING);
            writer.element("xs:element", "name", "LastDataUpdate", "minOccurs", "0", "type", "xs:dateTime");
            writer.element("xs:element", "name", "LastSchemaUpdate", "minOccurs", "0", "type", "xs:dateTime");

            writer.endElement(); // xsd:sequence
            writer.endElement(); // xsd:complexType
            writer.endElement(); // xsd:element name=Cube
         }

         writer.endElement(); // xsd:sequence
         writer.endElement(); // xsd:complexType
         writer.endElement(); // xsd:element name=CubeInfo
      }

      { // <AxesInfo>
         writer.startElement("xs:element", "name", "AxesInfo");
         writer.startElement("xs:complexType");
         writer.startElement("xs:sequence");
         { // <AxisInfo>
            writer.startElement("xs:element", "name", "AxisInfo", "maxOccurs", "unbounded");
            writer.startElement("xs:complexType");
            writer.startElement("xs:sequence");

            { // <HierarchyInfo>
               writer.startElement("xs:element", "name", "HierarchyInfo", "minOccurs", 0, "maxOccurs", "unbounded");
               writer.startElement("xs:complexType");
               writer.startElement("xs:sequence");
               writer.element("xs:any", "namespace", "##targetNamespace", "minOccurs", "0", "maxOccurs", "unbounded", "processContents", "skip");
               writer.endElement();// xs:sequence
               writer.element("xs:attribute", "name", "name", "type", "xs:string", "use", "required");
               writer.endElement(); // xs:complexType
               writer.endElement(); // xs:element: End HierarchyInfo
               writer.endElement(); // xs:sequence
               writer.element("xs:attribute", "name", "name", "type", "xs:string");
               writer.endElement(); // xs:complexType
               writer.endElement(); // xs:element: End AxisInfo
               writer.endElement(); // xs:sequence
               writer.endElement(); // xs:compleType
               writer.endElement(); // xs:element: End AxesInfo

            }
         }
      }


      // CellInfo

      { // <CellInfo>
         writer.startElement("xs:element", "name", "CellInfo");
         writer.startElement("xs:complexType");
         writer.startElement("xs:choice", "minOccurs", "0", "maxOccurs", "unbounded");
         writer.element("xs:any", "namespace", "##targetNamespace", "minOccurs", "0", "maxOccurs", "unbounded", "processContents", "skip");
         writer.endElement(); // xs:choice
         writer.endElement(); // xs:complexType
         writer.endElement(); // xs:element: End CellInfo
         writer.endElement();// xs:sequence
         writer.endElement();// xs:complexType: End OlapInfo

      }

   

      // Axes

      writer.startElement("xs:complexType", "name", "Axes");
      writer.startElement("xs:sequence");
      { // <Axis>
         writer.startElement("xs:element", "name", "Axis", "maxOccurs", "unbounded");

         writer.startElement("xs:complexType");
         writer.element("xs:group", "ref", "SetType", "minOccurs", "0", "maxOccurs", "unbounded");

         writer.element("xs:attribute", "name", "name", "type", "xs:string");

         writer.endElement(); // xsd:complexType

         // writer.element(
         // "xs:attribute",
         // "name", "name",
         // "type", XSD_STRING);
         writer.endElement(); // xs:element End Axis
         writer.endElement(); // xs:sequence
         writer.endElement(); // xs:complexType End Axes
      }

      // CellData

      writer.startElement("xs:complexType", "name", "CellData");
      writer.startElement("xs:sequence");
      { // <Cell>
         writer.startElement("xs:element", "name", "Cell", "minOccurs", 0, "maxOccurs", "unbounded");
         writer.startElement("xs:complexType");
         writer.startElement("xs:sequence");
         writer.element("xs:any", "namespace", "##targetNamespace", "minOccurs", "0", "maxOccurs", "unbounded", "processContents", "skip");
         writer.endElement(); // xs:sequence
         writer.element("xs:attribute", "name", "CellOrdinal", "type", XSD_UNSIGNED_INT, "use", "required");
         writer.endElement(); // xs:complexType
         writer.endElement(); // xs:element name=Cell
         writer.endElement(); // xs:sequence
         writer.endElement(); // xs:complexType
      }

      { // <root>
         writer.startElement("xs:element", "name", "root");
         writer.startElement("xs:complexType");
         writer.startElement("xs:sequence");
         writer.element("xs:any", "namespace", "http://www.w3.org/2001/XMLSchema", "minOccurs", "0", "processContents", "strict");

         writer.element("xs:element", "name", "OlapInfo", "type", "OlapInfo", "minOccurs", "0");
         writer.element("xs:element", "name", "Axes", "type", "Axes", "minOccurs", "0");
         writer.element("xs:element", "name", "CellData", "type", "CellData", "minOccurs", "0");
         writer.endElement(); // xs:sequence
         writer.endElement(); // xsd:complexType
         writer.endElement(); // xsd:element name=root
      }

      writer.endElement(); // xsd:schema
   }

   static void writeEmptyDatasetXmlSchema(SaxWriter writer, SetType setType) {
      String setNsXmla = NS_XMLA_ROWSET;
      writer.startElement("xs:schema", "xmlns:xs", NS_XSD, "targetNamespace", setNsXmla, "xmlns", setNsXmla, "xmlns:xsi", NS_XSI, "xmlns:sql", NS_XML_SQL, "elementFormDefault",
               "qualified");

      writer.element("xs:element", "name", "root");

      writer.endElement(); // xsd:schema
   }



   static class Column {
      final String name;
      final String encodedName;
      final String xsdType;

      Column(String name, int type, int scale) {
         this.name = name;

         // replace invalid XML element name, like " ", with "_x0020_" in
         // column headers, otherwise will generate a badly-formatted xml
         // doc.
         this.encodedName = XmlaUtil.ElementNameEncoder.INSTANCE.encode(name);
         this.xsdType = sqlToXsdType(type, scale);
      }
   }


   /**
    * Converts a SQL type to XSD type.
    * 
    * @param sqlType
    *           SQL type
    * @return XSD type
    */
   private static String sqlToXsdType(final int sqlType, final int scale) {
      switch (sqlType) {
      // Integer
      case Types.INTEGER:
      case Types.SMALLINT:
      case Types.TINYINT:
         return XSD_INT;
      case Types.NUMERIC:
      case Types.DECIMAL:
         /*
          * Oracle reports all numbers as NUMERIC. We check the scale of the
          * column and pick the right XSD type.
          */
         if (scale == 0) {
            return XSD_INT;
         } else {
            return XSD_DECIMAL;
         }
      case Types.BIGINT:
         return XSD_INTEGER;
         // Real
      case Types.DOUBLE:
      case Types.FLOAT:
         return XSD_DOUBLE;
         // Date and time
      case Types.TIME:
      case Types.TIMESTAMP:
      case Types.DATE:
         return XSD_STRING;
         // Other
      default:
         return XSD_STRING;
      }
   }
   /**
    * Pre-process each incoming mdx 
    * MSOLAP generates non-standard mdx frequently, which can't handled by olap4j driver. 
    * So this pre-process helps convert mdx in various ways
    */
   
   /**
    * Case 1.Trim off all double quote '""' 
    * Java String can't contain double quote '""' 
    * example input: WITH MEMBER [Deal_Dates_Start].[XL_PT0] AS 'strtomember("[Deal_Dates_Start].[2010]").UniqueName' MEMBER [Deal_Dates_Start].[XL_PT1] AS 'strtomember("[Deal_Dates_Start].[2009]").UniqueName' MEMBER [Deal_Dates_Start].[XL_PT2] AS 'strtomember("[Deal_Dates_Start].[2008]").UniqueName' SELECT {[Deal_Dates_Start].[XL_PT0],[Deal_Dates_Start].[XL_PT1],[Deal_Dates_Start].[XL_PT2]} ON 0 FROM Cashflows  CELL PROPERTIES VALUE
    * example output: WITH MEMBER [Deal_Dates_Start].[XL_PT0] AS 'strtomember([Deal_Dates_Start].[2010]).UniqueName' MEMBER [Deal_Dates_Start].[XL_PT1] AS 'strtomember([Deal_Dates_Start].[2009]).UniqueName' MEMBER [Deal_Dates_Start].[XL_PT2] AS 'strtomember([Deal_Dates_Start].[2008]).UniqueName' SELECT {[Deal_Dates_Start].[XL_PT0],[Deal_Dates_Start].[XL_PT1],[Deal_Dates_Start].[XL_PT2]} ON 0 FROM Cashflows  CELL PROPERTIES VALUE

    * If double quote is used inside text content, the escape "\" should be added before double quote. e.g. '\"' 
    * 
    * Case 2: Add missing table name
    * Mdx generated by MSOLAP driver may miss the cube name after FROM keyword. This happens only when you refresh the Pivot Table. Not sure what's going 
    * on under the hood, but we need to add current Cube Name after the FROM keyword.
    * 
    * example input SELECT ..... ,[Deal_Dates_Start].[XL_PT5]} ON 0 FROM   CELL PROPERTIES VALUE
    * example output SELECT ..... ,[Deal_Dates_Start].[XL_PT5]} ON 0 FROM [Sales] CELL PROPERTIES VALUE
    * The fix will add [Sales] after key word FROM
    * 
    * Case 3: We shouldn't display null value on Exce Pivot Table. This required all MDX SELECT statement be SELECT NON EMPTY 
    * example input:  SELECT ..... ,[Deal_Dates_Start].[XL_PT5]} ON 0 FROM [Sales] CELL PROPERTIES VALUE
    * example output:  SELECT NON EMPTY ..... ,[Deal_Dates_Start].[XL_PT5]} ON 0 FROM [Sales] CELL PROPERTIES VALUE
    */
   
   
   private  String preProcessMdx(String mdx) {
	      String mdxStr = mdx.replaceAll("\\s+","");
	      
	      //popuate the current cube if possible
	      if(mdxStr.contains("FROM[")){
	         int start = mdxStr.indexOf("FROM[");
	         String subMdxStr = mdxStr.substring(start);
	         int end = subMdxStr.indexOf("]") + start+1;
	         
	         currentCube = mdxStr.substring(start+5, end-1);
	         
	      }
	      
	      
	      //replace FROM CELL with FROM [Current_Cube] CELL
	      //Only process the last occurance, if more than one "FROMCELL" exists in the mdx
	      int fromCell=mdx.lastIndexOf("FROM  CELL");
	      if (fromCell>0){
	      String mdxP2 = mdx.substring(fromCell);
	      String mdxP1 = mdx.substring(0, fromCell-1);
	      
	      if(mdxP2.contains("FROM  CELL")){
	         mdxP2 = mdxP2.replace("FROM", "FROM " + currentCube);
	      }
	      mdx = mdxP1.concat(mdxP2);
	     }
	      
	      //enforce NON EMPTY in MDX statement
	      if(mdxStr.startsWith("SELECT")&& (!mdxStr.startsWith("SELECTFROM") && !mdxStr.startsWith("SELECTNONEMPTY"))) {
	         mdx = mdx.replace("SELECT", "SELECT NON EMPTY");
	      }      
	      
	      //replace the non escaped double quotes
	      mdx = mdx.replace("(\"[", "([");
	      mdx = mdx.replace("]\")", "])");
	      return mdx;
	      
	   }
   
   

   
   
private QueryResult executeQuery(XmlaRequest request) throws XmlaException {
      String mdx = preProcessMdx(request.getStatement());

      //if mdx statement contains CELL_ORDINAL properties, change the MDDataSet.cellPropLongs, and MDDataSet.cellPropLongs accordingly
      
      if(mdx.contains("CELL_ORDINAL")){
         MDDataSet.cellProps = Arrays.asList(MDDataSet.rename(StandardCellProperty.FORMAT_STRING, "FormatString")) ;
         MDDataSet.cellPropLongs = Arrays.asList(StandardCellProperty.CELL_ORDINAL) ;
      }
      else if (mdx.contains("VALUE") && mdx.contains("FORMAT_STRING") && mdx.contains("LANGUAGE")){
         MDDataSet.cellProps = Arrays.asList(MDDataSet.rename(StandardCellProperty.VALUE, "Value"), 
                  MDDataSet.rename(StandardCellProperty.FORMAT_STRING, "FormatString"),
                  MDDataSet.rename(StandardCellProperty.LANGUAGE, "Language"));
         MDDataSet.cellPropLongs = Arrays
                  .asList(StandardCellProperty.VALUE, StandardCellProperty.FORMAT_STRING, StandardCellProperty.LANGUAGE);
      }
      else {
         MDDataSet.cellProps = Arrays.asList(MDDataSet.rename(StandardCellProperty.VALUE, "Value"));
         MDDataSet.cellPropLongs = Arrays
                  .asList(StandardCellProperty.VALUE);
      }
      if (LOGGER.isDebugEnabled()) {
         
         LOGGER.debug("mdx: \"" + mdx + "\"");
      }

      if ((mdx == null) || (mdx.length() == 0)) {
         return null;
      }
      checkFormat(request);

      OlapConnection connection = null;
      PreparedOlapStatement statement = null;
      CellSet cellSet = null;
      boolean success = false;
      try {
         
         
         connection = getConnection(request, Collections.<String, String> emptyMap());
         getExtra(connection).setPreferList(connection);
         try {
            if(mdx.startsWith("REFRESH"))
               mdx = lastMdx;
            statement = connection.prepareOlapStatement(mdx);
            lastMdx = mdx;

         } catch (XmlaException ex) {
            throw ex;
         } catch (Exception ex) {
            throw new XmlaException(CLIENT_FAULT_FC, HSB_PARSE_QUERY_CODE, HSB_PARSE_QUERY_FAULT_FS, ex);
         }
         try {

            RolapConnection rolapConn = ((Statement) statement).getMondrianConnection();
            if (factCacheControl == null) {
               factCacheControl = initializeFactCacheControl(rolapConn);
            }

            /**
             * We should only cache Dimension data in Mondrian Olap. But Mondrian cache both Dimension caches and Fact caches by default, 
             * here we need to flush fact table measures for every mdx query. Otherwise Mondrian only returns cached results. 
             */
            if (ExternalProperties.getInstance().isDisableMeasuresCashing()) {
               // If client choose to disable caching for fact cache measures
               mondrian.olap.Cube cube = ((Statement) statement).getQuery().getCube();
               currentCube = cube.getName();
               CellRegion currentCubeRegion = factCacheControl.createMeasuresRegion(cube);
               this.factCacheControl.flush(currentCubeRegion);
            }
            if (ExternalProperties.getInstance().isDisableSchemaCaching()) {

               this.factCacheControl.flushSchemaCache();
            }
            cellSet = statement.executeQuery();
           
            
            final Format format = getFormat(request, null);
            final Content content = getContent(request);
            final Enumeration.ResponseMimeType responseMimeType = getResponseMimeType(request);
            final MDDataSet dataSet;

            if (format == Format.Multidimensional) {
               dataSet = new MDDataSet_Multidimensional(cellSet, content != Content.DataIncludeDefaultSlicer, responseMimeType == Enumeration.ResponseMimeType.JSON);
            } else {
               dataSet = new MDDataSet_Tabular(cellSet);
            }
            success = true;
            return dataSet;
         } catch (XmlaException ex) {
            throw ex;
         } catch (Exception ex) {
            throw new XmlaException(SERVER_FAULT_FC, HSB_EXECUTE_QUERY_CODE, HSB_EXECUTE_QUERY_FAULT_FS, ex);
         }
      } finally {
         if (!success) {
            if (cellSet != null) {
               try {
                  cellSet.close();
               } catch (SQLException e) {
                  // ignore
               }
            }
            if (statement != null) {
               try {
                  statement.close();
               } catch (SQLException e) {
                  // ignore
               }
            }
            if (connection != null) {
               try {
                  connection.close();
               } catch (SQLException e) {
                  // ignore
               }
            }
         }
      }
   }
   
   private QueryResult executeDrillThroughQuery(XmlaRequest request) throws XmlaException {
      checkFormat(request);

      final Map<String, String> properties = request.getProperties();
      String tabFields = properties.get(PropertyDefinition.TableFields.name());
      if (tabFields != null && tabFields.length() == 0) {
         tabFields = null;
      }
      final String advancedFlag = properties.get(PropertyDefinition.AdvancedFlag.name());
      final boolean advanced = Boolean.parseBoolean(advancedFlag);
      final boolean enableRowCount = MondrianProperties.instance().EnableTotalCount.booleanValue();
      final int[] rowCountSlot = enableRowCount ? new int[] { 0 } : null;
      OlapConnection connection = null;
      OlapStatement statement = null;
      ResultSet resultSet = null;
      try {
         connection = getConnection(request, Collections.<String, String> emptyMap());
         statement = connection.createStatement();
         resultSet = getExtra(connection).executeDrillthrough(statement, request.getStatement(), advanced, tabFields, rowCountSlot);
         int rowCount = enableRowCount ? rowCountSlot[0] : -1;
         return new TabularRowSet(resultSet, rowCount);
      } catch (XmlaException xex) {
         throw xex;
      } catch (SQLException sqle) {
         throw new XmlaException(SERVER_FAULT_FC, HSB_DRILL_THROUGH_SQL_CODE, HSB_DRILL_THROUGH_SQL_FAULT_FS, Util.newError(sqle, "Error in drill through"));
      } catch (RuntimeException e) {
         // NOTE: One important error is "cannot drill through on the cell"
         throw new XmlaException(SERVER_FAULT_FC, HSB_DRILL_THROUGH_SQL_CODE, HSB_DRILL_THROUGH_SQL_FAULT_FS, e);
      } finally {
         if (resultSet != null) {
            try {
               resultSet.close();
            } catch (SQLException e) {
               // ignore
            }
         }
         if (statement != null) {
            try {
               statement.close();
            } catch (SQLException e) {
               // ignore
            }
         }
         if (connection != null) {
            try {
               connection.close();
            } catch (SQLException e) {
               // ignore
            }
         }
      }
   }

   private static Format getFormat(XmlaRequest request, Format defaultValue) {
      final String formatName = (String) request.getProperties().get(PropertyDefinition.Format.name());
      return Util.lookup(Format.class, formatName, defaultValue);
   }

   private static Content getContent(XmlaRequest request) {
      final String contentName = (String) request.getProperties().get(PropertyDefinition.Content.name());
      return Util.lookup(Content.class, contentName, Content.DEFAULT);
   }

   private static Enumeration.ResponseMimeType getResponseMimeType(XmlaRequest request) {
      Enumeration.ResponseMimeType mimeType = (ResponseMimeType) Enumeration.ResponseMimeType.MAP.get(request.getProperties().get(PropertyDefinition.ResponseMimeType.name()));
      if (mimeType == null) {
         mimeType = Enumeration.ResponseMimeType.SOAP;
      }
      return mimeType;
   }

   static abstract class ColumnHandler {
      protected final String name;
      protected final String encodedName;

      protected ColumnHandler(String name) {
         this.name = name;
         this.encodedName = XmlaUtil.ElementNameEncoder.INSTANCE.encode(name);
      }

      abstract void write(SaxWriter writer, Cell cell, Member[] members) throws OlapException;

      abstract void metadata(SaxWriter writer);
   }

   /**
    * Callback to handle one column, representing the combination of a level and
    * a property (e.g. [Store].[Store State].[MEMBER_UNIQUE_NAME]) in a
    * flattened dataset.
    */
   static class CellColumnHandler extends ColumnHandler {

      CellColumnHandler(String name) {
         super(name);
      }

      public void metadata(SaxWriter writer) {
         writer.element("xs:element", "minOccurs", 0, "name", encodedName, "sql:field", name);
      }

      public void write(SaxWriter writer, Cell cell, Member[] members) {
         if (cell.isNull()) {
            return;
         }
         Object value = cell.getValue();
         final String dataType = (String) cell.getPropertyValue(StandardCellProperty.DATATYPE);

         final ValueInfo vi = new ValueInfo(dataType, value);
         final String valueType = vi.valueType;
         value = vi.value;
         boolean isDecimal = vi.isDecimal;

         String valueString = value.toString();

         writer.startElement(encodedName, "xsi:type", valueType);
         if (isDecimal) {
            valueString = XmlaUtil.normalizeNumericString(valueString, true, 3);
         }
         writer.characters(valueString);
         writer.endElement();
      }
   }

   /**
    * Callback to handle one column, representing the combination of a level and
    * a property (e.g. [Store].[Store State].[MEMBER_UNIQUE_NAME]) in a
    * flattened dataset.
    */
   static class MemberColumnHandler extends ColumnHandler {
      private final Property property;
      private final Level level;
      private final int memberOrdinal;

      public MemberColumnHandler(Property property, Level level, int memberOrdinal) {
         super(level.getUniqueName() + "." + Util.quoteMdxIdentifier(property.getName()));
         this.property = property;
         this.level = level;
         this.memberOrdinal = memberOrdinal;
      }

      public void metadata(SaxWriter writer) {
         writer.element("xs:element", "minOccurs", 0, "name", encodedName, "sql:field", name, "type", XSD_STRING);
      }

      public void write(SaxWriter writer, Cell cell, Member[] members) throws OlapException {
         Member member = members[memberOrdinal];
         final int depth = level.getDepth();
         if (member.getDepth() < depth) {
            // This column deals with a level below the current member.
            // There is no value to write.
            return;
         }
         while (member.getDepth() > depth) {
            member = member.getParentMember();
         }
         final Object propertyValue = member.getPropertyValue(property);
         if (propertyValue == null) {
            return;
         }

         writer.startElement(encodedName);
         writer.characters(propertyValue.toString());
         writer.endElement();
      }
   }

   
   /**
    * Discover request handler
    * @param request
    * @param response
    * @throws XmlaException
    */
   private void discover(XmlaRequest request, XmlaResponse response) throws XmlaException {
      final RowsetDefinition rowsetDefinition = RowsetDefinition.valueOf(request.getRequestType());
      ((DefaultXmlaRequest)request).setRequestItemName(rowsetDefinition.name());
      
      Rowset rowset = rowsetDefinition.getRowset(request, this);
      
      // put the current cube name to the header of XMLA response. Then we're able to trace the name of current cube.
      if(rowsetDefinition == RowsetDefinition.MDSCHEMA_HIERARCHIES && !rowset.getRestrictions().containsKey("CUBE_NAME")){
       ((DefaultXmlaRequest)request).setCurrentCube(currentCube);
       ((DefaultXmlaRequest)request).putRestriction("CUBE_NAME", currentCube);
      }
      if(currentCube != null && currentCube.length()>1){
         ((DefaultXmlaRequest)request).setCurrentCube(currentCube);
         ((DefaultXmlaResponse)response).setCurrentCube(currentCube);

      }
      Format format = getFormat(request, Format.Tabular);
      if (format != Format.Tabular) {
         throw new XmlaException(CLIENT_FAULT_FC, HSB_DISCOVER_FORMAT_CODE, HSB_DISCOVER_FORMAT_FAULT_FS, new UnsupportedOperationException(
                  "<Format>: only 'Tabular' allowed in Discover method " + "type"));
      }
      final Content content = getContent(request);

      SaxWriter writer = response.getWriter();

      writer.startDocument();
      writer.startElement("DiscoverResponse", "xmlns", NS_XMLA, "xmlns:ddl2", NS_XMLA_DDL2, "xmlns:ddl2_2", NS_XMLA_DDL2_2, "xmlns:ddl100", NS_XMLA_DDL100, "xmlns:ddl100_100",
               NS_XMLA_DDL100_100, "xmlns:ddl200", NS_XMLA_DDL200, "xmlns:ddl200_200", NS_XMLA_DDL200_200, "xmlns:ddl300", NS_XMLA_DDL300, "xmlns:ddl300_300", NS_XMLA_DDL300_300,
               "xmlns:ddl400", NS_XMLA_DDL400, "xmlns:ddl400_400", NS_XMLA_DDL400_400);

      writer.startElement("return");
      writer.startElement("root", "xmlns", NS_XMLA_ROWSET, "xmlns:xsi", NS_XSI, "xmlns:xsd", NS_XSD, "xmlns:msxmla", MS_XMLA);

      switch (content) {
      case Schema:
      case SchemaData:
         rowset.rowsetDefinition.writeRowsetXmlSchema(writer);
         break;
      }

      try {
         switch (content) {
         case Data:
         case SchemaData:
            rowset.unparse(response);
            break;
         }
      } catch (XmlaException xex) {
         throw xex;
      } catch (Throwable t) {
         throw new XmlaException(SERVER_FAULT_FC, HSB_DISCOVER_UNPARSE_CODE, HSB_DISCOVER_UNPARSE_FAULT_FS, t);
      } finally {
         // keep the tags balanced, even if there's an error
         try {
            writer.endElement();
            writer.endElement();
            writer.endElement();
         } catch (Throwable e) {
            // Ignore any errors balancing the tags. The original exception
            // is more important.
         }
      }

      writer.endDocument();
   }

   /**
    * Gets a Connection given a catalog (and implicitly the catalog's data
    * source) and the name of a user role.
    * 
    * <p>
    * If you want to pass in a role object, and you are making the call within
    * the same JVM (i.e. not RPC), register the role using
    * {@link mondrian.olap.MondrianServer#getLockBox()} and pass in the moniker
    * for the generated lock box entry. The server will retrieve the role from
    * the moniker.
    * 
    * @param catalog
    *           Catalog name
    * @param schema
    *           Schema name
    * @param role
    *           User role name
    * @return Connection
    * @throws XmlaException
    *            If error occurs
    */
   protected OlapConnection getConnection(String catalog, String schema, final String role) throws XmlaException {
      return this.getConnection(catalog, schema, role, new Properties());
   }

   /**
    * Gets a Connection given a catalog (and implicitly the catalog's data
    * source) and the name of a user role.
    * 
    * <p>
    * If you want to pass in a role object, and you are making the call within
    * the same JVM (i.e. not RPC), register the role using
    * {@link mondrian.olap.MondrianServer#getLockBox()} and pass in the moniker
    * for the generated lock box entry. The server will retrieve the role from
    * the moniker.
    * 
    * @param catalog
    *           Catalog name
    * @param schema
    *           Schema name
    * @param role
    *           User role name
    * @param props
    *           Properties to pass down to the native driver.
    * @return Connection
    * @throws XmlaException
    *            If error occurs
    */
   protected OlapConnection getConnection(String catalog, String schema, final String role, Properties props) throws XmlaException {
      try {
         //TODO
         //Currently disable the role
         return connectionFactory.getConnection(catalog, schema, null, props);
      } catch (SecurityException e) {
         throw new XmlaException(CLIENT_FAULT_FC, HSB_ACCESS_DENIED_CODE, HSB_ACCESS_DENIED_FAULT_FS, e);
      } catch (SQLException e) {
         throw new XmlaException(CLIENT_FAULT_FC, HSB_CONNECTION_DATA_SOURCE_CODE, HSB_CONNECTION_DATA_SOURCE_FAULT_FS, e);
      }
   }

      static class IntList extends AbstractList<Integer> {
      private final int[] ints;

      IntList(int[] ints) {
         this.ints = ints;
      }

      public Integer get(int index) {
         return ints[index];
      }

      public int size() {
         return ints.length;
      }
   }

   /**
    * Default implementation of {@link mondrian.xmla.XmlaHandler.XmlaExtra}.
    * Connections based on mondrian's olap4j driver can do better.
    */
   private static class XmlaExtraImpl implements XmlaExtra {
      public ResultSet executeDrillthrough(OlapStatement olapStatement, String mdx, boolean advanced, String tabFields, int[] rowCountSlot) throws SQLException {
         return olapStatement.executeQuery(mdx);
      }

      public void setPreferList(OlapConnection connection) {
         // ignore
      }

      public Date getSchemaLoadDate(Schema schema) {
         return new Date();
      }

      public int getLevelCardinality(Level level) {
         return level.getCardinality();
      }

      public void getSchemaFunctionList(List<FunctionDefinition> funDefs, Schema schema, Util.Functor1<Boolean, String> functionFilter) {
         // no function definitions
      }

      public int getHierarchyCardinality(Hierarchy hierarchy) {
         int cardinality = 0;
         for (Level level : hierarchy.getLevels()) {
            cardinality += level.getCardinality();
         }
         return cardinality;
      }

      public int getHierarchyStructure(Hierarchy hierarchy) {
         return 0;
      }

      public boolean isHierarchyParentChild(Hierarchy hierarchy) {
         return false;
      }

      public int getMeasureAggregator(Member member) {
         return RowsetDefinition.MdschemaMeasuresRowset.MDMEASURE_AGGR_UNKNOWN;
      }

      public void checkMemberOrdinal(Member member) {
         // nothing to do
      }

      public boolean shouldReturnCellProperty(CellSet cellSet, Property cellProperty, boolean evenEmpty) {
         return true;
      }

      public List<String> getSchemaRoleNames(Schema schema) {
         return Collections.emptyList();
      }

      public String getCubeType(Cube cube) {
         return RowsetDefinition.MdschemaCubesRowset.MD_CUBTYPE_CUBE;
      }

      public boolean isLevelUnique(Level level) {
         return false;
      }

      public List<Property> getLevelProperties(Level level) {
         return level.getProperties();
      }

      public boolean isPropertyInternal(Property property) {
         return property instanceof Property.StandardMemberProperty && ((Property.StandardMemberProperty) property).isInternal()
                  || property instanceof Property.StandardCellProperty && ((Property.StandardCellProperty) property).isInternal();
      }

      public List<Map<String, Object>> getDataSources(OlapConnection connection) throws OlapException {
         Database olapDb = connection.getOlapDatabase();
         final String modes = createCsv(olapDb.getAuthenticationModes());
         final String providerTypes = createCsv(olapDb.getProviderTypes());
         return Collections.singletonList(Olap4jUtil.mapOf("DataSourceName", (Object) olapDb.getName(), "DataSourceDescription", olapDb.getDescription(), "URL", olapDb.getURL(),
                  "DataSourceInfo", olapDb.getDataSourceInfo(), "ProviderName", olapDb.getProviderName(), "ProviderType", providerTypes, "AuthenticationMode", modes));
      }

      public Map<String, Object> getAnnotationMap(MetadataElement element) {
         return Collections.emptyMap();
      }
   }

   private static String createCsv(Iterable<? extends Object> iterable) {
      StringBuilder sb = new StringBuilder();
      boolean first = true;
      for (Object o : iterable) {
         if (!first) {
            sb.append(',');
         }
         sb.append(o);
         first = false;
      }
      return sb.toString();
   }

   CacheControl initializeFactCacheControl(RolapConnection connection) {
      try {
         factCacheControl = connection.getCacheControl(new PrintWriter("factCacheControl.txt"));

      } catch (FileNotFoundException e) {
         e.printStackTrace();
      }
      return factCacheControl;

   }
}

// End XmlaHandler.java
