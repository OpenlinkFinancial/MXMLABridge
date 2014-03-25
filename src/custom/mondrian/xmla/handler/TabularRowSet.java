/*
 * Copyright (c) 2008-2014 Open Link Financial, Inc. All Rights Reserved.
 */

package custom.mondrian.xmla.handler;

import static custom.mondrian.xmla.handler.XmlaConstants.NS_XMLA_ROWSET;
import static custom.mondrian.xmla.handler.XmlaConstants.NS_XSD;
import static custom.mondrian.xmla.handler.XmlaConstants.NS_XSI;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.xml.sax.SAXException;

import custom.mondrian.xmla.handler.CustomXmlaHandler.Column;
import custom.mondrian.xmla.writer.SaxWriter;

/**
 * <class description goes here>
 * 
 */

class TabularRowSet extends QueryResult {
   private final List<Column> columns = new ArrayList<Column>();
   private final List<Object[]> rows;
   private int totalCount;

   /**
    * Creates a TabularRowSet based upon a SQL statement result.
    * 
    * <p>
    * Does not close the ResultSet, on success or failure. Client must do it.
    * 
    * @param rs
    *           Result set
    * @param totalCount
    *           Total number of rows. If >= 0, writes the "totalCount"
    *           attribute into the XMLA response.
    * 
    * @throws SQLException
    *            on error
    */
   public TabularRowSet(ResultSet rs, int totalCount) throws SQLException {
      this.totalCount = totalCount;
      ResultSetMetaData md = rs.getMetaData();
      int columnCount = md.getColumnCount();

      // populate column defs
      for (int i = 0; i < columnCount; i++) {
         columns.add(new Column(md.getColumnLabel(i + 1), md.getColumnType(i + 1), md.getScale(i + 1)));
      }

      // Populate data; assume that SqlStatement is already positioned
      // on first row (or isDone() is true), and assume that the
      // number of rows returned is limited.
      rows = new ArrayList<Object[]>();
      while (rs.next()) {
         Object[] row = new Object[columnCount];
         for (int i = 0; i < columnCount; i++) {
            Object obj = rs.getObject(i + 1);
            // escape the null value. Excel couldn't handle null value.
            if (obj != null)
               row[i] = rs.getObject(i + 1);
            else if (!(obj instanceof String)) {
               row[i] = 0;
            }
         }
         rows.add(row);
      }
   }

   /**
    * Alternate constructor for advanced drill-through.
    * 
    * @param tableFieldMap
    *           Map from table name to a list of the names of the fields in
    *           the table
    * @param tableList
    *           List of table names
    */
   public TabularRowSet(Map<String, List<String>> tableFieldMap, List<String> tableList) {
      for (String tableName : tableList) {
         List<String> fieldNames = tableFieldMap.get(tableName);
         for (String fieldName : fieldNames) {
            columns.add(new Column(tableName + "." + fieldName, Types.VARCHAR, 0));
         }
      }

      rows = new ArrayList<Object[]>();
      Object[] row = new Object[columns.size()];
      for (int k = 0; k < row.length; k++) {
         row[k] = k;
      }
      rows.add(row);
   }

   public void close() {
      // no resources to close
   }

   public void unparse(SaxWriter writer) throws SAXException {
      // write total count row if enabled
      if (totalCount >= 0) {
         String countStr = Integer.toString(totalCount);
         writer.startElement("row");
         for (Column column : columns) {
            writer.startElement(column.encodedName);
            writer.characters(countStr);
            writer.endElement();
         }
         writer.endElement(); // row
      }

      for (Object[] row : rows) {
         writer.startElement("row");
         for (int i = 0; i < row.length; i++) {
            writer.startElement(columns.get(i).encodedName, new Object[] { "xsi:type", columns.get(i).xsdType });
            Object value = row[i];
            if (value == null) {
               writer.characters("null");
            } else {
               String valueString = value.toString();
               if (value instanceof Number) {
                  // valueString =
                  // XmlaUtil.normalizeNumericString(valueString);
               }
               writer.characters(valueString);
            }
            writer.endElement();
         }
         writer.endElement(); // row
      }
   }

   /**
    * Writes the tabular drillthrough schema
    * 
    * @param writer
    *           Writer
    */
   public void metadata(SaxWriter writer) {
      writer.startElement("xs:schema", "xmlns:xs", NS_XSD, "targetNamespace", NS_XMLA_ROWSET, "xmlns", NS_XMLA_ROWSET, "xmlns:xsi", NS_XSI, "xmlns:sql", CustomXmlaHandler.NS_XML_SQL,
               "elementFormDefault", "qualified");

      { // <root>
         writer.startElement("xs:element", "name", "root");
         writer.startElement("xs:complexType");
         writer.startElement("xs:sequence");
         writer.element("xs:element", "maxOccurs", "unbounded", "minOccurs", 0, "name", "row", "type", "row");
         writer.endElement(); // xs:sequence
         writer.endElement(); // xsd:complexType
         writer.endElement(); // xsd:element name=root
      }

      { // xsd:simpleType name="uuid"
         writer.startElement("xs:simpleType", "name", "uuid");
         writer.startElement("xs:restriction", "base", CustomXmlaHandler.XSD_STRING);
         writer.element("xs:pattern", "value", RowsetDefinition.UUID_PATTERN);
         writer.endElement(); // xs:restriction
         writer.endElement(); // xsd:simpleType
      }

      { // xsd:complexType name="row"
         writer.startElement("xs:complexType", "name", "row");
         writer.startElement("xs:sequence");
         for (Column column : columns) {
            writer.element("xs:element", "minOccurs", 0, "name", column.encodedName, "sql:field", column.name, "type", column.xsdType);
         }

         writer.endElement(); // xsd:sequence
         writer.endElement(); // xsd:complexType
      }
      writer.endElement(); // xsd:schema
   }
}
