/*
 * Copyright (c) 2008-2014 Open Link Financial, Inc. All Rights Reserved.
 */

package custom.mondrian.xmla.handler;

import static custom.mondrian.xmla.handler.XmlaConstants.NS_XMLA_ROWSET;
import static custom.mondrian.xmla.handler.XmlaConstants.NS_XSD;
import static custom.mondrian.xmla.handler.XmlaConstants.NS_XSI;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mondrian.olap.Util;
import mondrian.util.CompositeList;
import mondrian.xmla.XmlaHandler.XmlaExtra;

import org.apache.log4j.Logger;
import org.olap4j.Cell;
import org.olap4j.CellSet;
import org.olap4j.CellSetAxis;
import org.olap4j.CellSetAxisMetaData;
import org.olap4j.OlapException;
import org.olap4j.Position;
import org.olap4j.metadata.Cube;
import org.olap4j.metadata.Datatype;
import org.olap4j.metadata.Dimension;
import org.olap4j.metadata.Hierarchy;
import org.olap4j.metadata.Level;
import org.olap4j.metadata.Member;
import org.olap4j.metadata.Property;
import org.olap4j.metadata.Property.StandardCellProperty;
import org.olap4j.metadata.Property.StandardMemberProperty;
import org.xml.sax.SAXException;

import custom.mondrian.xmla.handler.CustomXmlaHandler.CellColumnHandler;
import custom.mondrian.xmla.handler.CustomXmlaHandler.ColumnHandler;
import custom.mondrian.xmla.handler.CustomXmlaHandler.MemberColumnHandler;
import custom.mondrian.xmla.handler.CustomXmlaHandler.ValueInfo;
import custom.mondrian.xmla.writer.SaxWriter;

/**
 * <class description goes here>
 * 
 */

abstract class MDDataSet extends QueryResult {
   
   @SuppressWarnings("unused")
   private static final Logger LOGGER = Logger.getLogger(MDDataSet.class);
   protected final CellSet cellSet;

   protected static  List<Property> cellProps = Arrays.asList(rename(StandardCellProperty.VALUE, "Value"), 
            rename(StandardCellProperty.FORMAT_STRING, "FormatString"),
            rename(StandardCellProperty.LANGUAGE, "Language"),
            rename(StandardCellProperty.CELL_ORDINAL,"CellOrdinal"));

   protected static  List<StandardCellProperty> cellPropLongs = Arrays
            .asList(StandardCellProperty.VALUE, StandardCellProperty.FORMAT_STRING, StandardCellProperty.LANGUAGE, StandardCellProperty.CELL_ORDINAL);

   protected static final List<Property> defaultProps = Arrays.asList(rename(StandardMemberProperty.MEMBER_UNIQUE_NAME, "UName"),
            rename(StandardMemberProperty.MEMBER_CAPTION, "Caption"), rename(StandardMemberProperty.LEVEL_UNIQUE_NAME, "LName"),
            rename(StandardMemberProperty.LEVEL_NUMBER, "LNum"), rename(StandardMemberProperty.DISPLAY_INFO, "DisplayInfo"));

   protected static final Map<String, StandardMemberProperty> longProps = new HashMap<String, StandardMemberProperty>();

   static {
      longProps.put("UName", StandardMemberProperty.MEMBER_UNIQUE_NAME);
      longProps.put("Caption", StandardMemberProperty.MEMBER_CAPTION);
      longProps.put("LName", StandardMemberProperty.LEVEL_UNIQUE_NAME);
      longProps.put("LNum", StandardMemberProperty.LEVEL_NUMBER);
      longProps.put("DisplayInfo", StandardMemberProperty.DISPLAY_INFO);
   }

   protected MDDataSet(CellSet cellSet) {
      this.cellSet = cellSet;
   }

   public void close() throws SQLException {
      cellSet.getStatement().getConnection().close();
   }

    static Property rename(final Property property, final String name) {
      return new Property() {
         public Datatype getDatatype() {
            return property.getDatatype();
         }

         public Set<TypeFlag> getType() {
            return property.getType();
         }

         public ContentType getContentType() {

            return property.getContentType();
         }

         public String getName() {
            return name;
         }

         public String getUniqueName() {
            return property.getUniqueName();
         }

         public String getCaption() {
            return property.getCaption();
         }

         public String getDescription() {
            return property.getDescription();
         }

         public boolean isVisible() {
            return property.isVisible();
         }
      };
   }
}

class MDDataSet_Multidimensional extends MDDataSet {
   private static final Logger LOGGER = Logger.getLogger(MDDataSet_Multidimensional.class);

   private List<Hierarchy> slicerAxisHierarchies;
   private final boolean omitDefaultSlicerInfo;
   private final boolean json;
   private XmlaUtil.ElementNameEncoder encoder = XmlaUtil.ElementNameEncoder.INSTANCE;
   private XmlaExtra extra;

   protected MDDataSet_Multidimensional(CellSet cellSet, boolean omitDefaultSlicerInfo, boolean json) throws SQLException {
      super(cellSet);
      this.omitDefaultSlicerInfo = omitDefaultSlicerInfo;
      this.json = json;
      this.extra = CustomXmlaHandler.getExtra(cellSet.getStatement().getConnection());
   
   }

   public void unparse(SaxWriter writer) throws SAXException, OlapException {
      olapInfo(writer);
      axes(writer);
      cellData(writer);
   }

   public void metadata(SaxWriter writer) {
      writer.verbatim(CustomXmlaHandler.MD_DATA_SET_XML_SCHEMA);
   }

   private void olapInfo(SaxWriter writer) throws OlapException {
      // What are all of the cube's hierachies
      Cube cube = cellSet.getMetaData().getCube();

      writer.startElement("OlapInfo");
      writer.startElement("CubeInfo");
      writer.startElement("Cube");
      writer.textElement("CubeName", cube.getName());
      writer.startElement("LastDataUpdate", "xmln", "http://schemas.microsoft.com/analysisservices/2003/engine");
      // The Date is fake,because currently Mondrian doesn't support record
      // LastDateUpdate
      writer.characters("2009-05-30T19:26:25");
      writer.endElement();// End LastDateUpdate
      writer.startElement("LastSchemaUpdate", "xmln", "http://schemas.microsoft.com/analysisservices/2003/engine");

      // The Date is fake,because currently Mondrian doesn't support record
      // LastDateUpdate
      writer.characters("2009-05-30T19:26:25");
      writer.endElement();// End LastSchemaUpdate
      writer.endElement();
      writer.endElement(); // CubeInfo

      // create AxesInfo for axes
      // -----------
      writer.startSequence("AxesInfo", "AxisInfo");
      final List<CellSetAxis> axes = cellSet.getAxes();
      List<Hierarchy> axisHierarchyList = new ArrayList<Hierarchy>();
      for (int i = 0; i < axes.size(); i++) {
         List<Hierarchy> hiers = axisInfo(writer, axes.get(i), "Axis" + i);
         axisHierarchyList.addAll(hiers);
      }
      // /////////////////////////////////////////////
      // create AxesInfo for slicer axes
      //
      
//      Object tmpAxis = axisList.get(0);
//      Object tmp1 = ((CellSetAxis)tmpAxis).getAxisOrdinal();
//      Object tmp2 = ((CellSetAxis)tmpAxis).getCellSet();
//      Object tmp3 = ((CellSetAxis)tmpAxis).getAxisMetaData();
//      List<Position> positions = ((CellSetAxis)tmpAxis).getPositions();
//      int tmpCount = positions.size();
//      Position tmpPosition = positions.get(0);
//      List<Member> members = positions.get(0).getMembers();
      List<Hierarchy> hierarchies;
      CellSetAxis slicerAxis = cellSet.getFilterAxis();
      if (omitDefaultSlicerInfo) {
         hierarchies = axisInfo(writer, slicerAxis, "SlicerAxis");
      } else {
         // The slicer axes contains the default hierarchy
         // of each dimension not seen on another axis.
         List<Dimension> unseenDimensionList = new ArrayList<Dimension>(cube.getDimensions());
         for (Hierarchy hier1 : axisHierarchyList) {
            unseenDimensionList.remove(hier1.getDimension());
         }
         hierarchies = new ArrayList<Hierarchy>();
         for (Dimension dimension : unseenDimensionList) {
            for (Hierarchy hierarchy : dimension.getHierarchies()) {
               hierarchies.add(hierarchy);
            }
         }
         writer.startElement("AxisInfo", "name", "SlicerAxis");
         writeHierarchyInfo(writer, hierarchies, getProps(slicerAxis.getAxisMetaData()));
         writer.endElement(); // AxisInfo
      }
      slicerAxisHierarchies = hierarchies;
      //
      // /////////////////////////////////////////////

      writer.endSequence(); // AxesInfo

      // -----------
      writer.startElement("CellInfo");
      cellProperty(writer, StandardCellProperty.VALUE, true, "Value");

      cellProperty(writer, StandardCellProperty.FORMAT_STRING, true, "FormatString");

      cellProperty(writer, StandardCellProperty.LANGUAGE, false, "Language");
      cellProperty(writer, StandardCellProperty.CELL_ORDINAL, false, "CellOrdinal");

      cellProperty(writer, StandardCellProperty.BACK_COLOR, false, "BackColor");
      cellProperty(writer, StandardCellProperty.FORE_COLOR, false, "ForeColor");
      cellProperty(writer, StandardCellProperty.FONT_FLAGS, false, "FontFlags");
      writer.endElement(); // CellInfo
      // -----------
      writer.endElement(); // OlapInfo
   }

   private void cellProperty(SaxWriter writer, StandardCellProperty cellProperty, boolean evenEmpty, String elementName) {
      Datatype type = null;

      // by default MondrianOlap4j won't return the FORMAT_STRING, LANGUAGE
      // properties to XMLA clients.
      // to make Mondrian compatible with MSOLAP driver, we need have to add
      // them here
      if (elementName.equalsIgnoreCase("formatstring")) {
         type = Datatype.STRING;
         writer.element(elementName, "name", "FORMAT_STRING", "type", getXsdTypeByDatatype(type));
      } else if (elementName.equalsIgnoreCase("language")) {
         type = Datatype.UNSIGNED_INTEGER;
         writer.element(elementName, "name", "LANGUAGE", "type", getXsdTypeByDatatype(type));
      } else if (elementName.equalsIgnoreCase("cellordinal")){
         type = Datatype.STRING;
         writer.element(elementName, "name", "CellOrdinal", "type", getXsdTypeByDatatype(type));
      }
      else if (extra.shouldReturnCellProperty(cellSet, cellProperty, evenEmpty)) {
         if (elementName.equals("Value")) {
            writer.element(elementName, "name", cellProperty.getName());
            return;
         }
         if (elementName.equals("BACK_COLOR") || elementName.equals("FORE_COLOR")) {
            type = Datatype.UNSIGNED_INTEGER;
         } else if (elementName.equals("FONT_FLAGS")) {
            type = Datatype.INTEGER;
         } else {
            type = cellProperty.getDatatype();
         }
         writer.element(elementName, "name", cellProperty.getName(), "type", getXsdTypeByDatatype(type));
      }
   }

   private List<Hierarchy> axisInfo(SaxWriter writer, CellSetAxis axis, String axisName) {
      writer.startElement("AxisInfo", "name", axisName);

      List<Hierarchy> hierarchies;
      Iterator<org.olap4j.Position> it = axis.getPositions().iterator();
      if (it.hasNext()) {
         final org.olap4j.Position position = it.next();
         hierarchies = new ArrayList<Hierarchy>();
         for (Member member : position.getMembers()) {
            hierarchies.add(member.getHierarchy());
         }
      } else {
         hierarchies = axis.getAxisMetaData().getHierarchies();
      }
      List<Property> props = getProps(axis.getAxisMetaData());
      writeHierarchyInfo(writer, hierarchies, props);

      writer.endElement(); // AxisInfo

      return hierarchies;
   }

   private void writeHierarchyInfo(SaxWriter writer, List<Hierarchy> hierarchies, List<Property> props) {
      writer.startSequence(null, "HierarchyInfo");
      for (Hierarchy hierarchy : hierarchies) {
         writer.startElement("HierarchyInfo", "name", "[" + hierarchy.getName() + "]");

         // exclude props "PARENT_UNIQUE_NAME" and "HIERARCHY_UNIQUE_NAME" by
         // exlcuding props[1]
         for (final Property prop : props) {
            if (!prop.getName().equals("PARENT_UNIQUE_NAME") && !prop.getName().equals("HIERARCHY_UNIQUE_NAME")) {
               final String encodedProp = encoder.encode(prop.getName());
               final Object[] attributes = getAttributes(prop, hierarchy);
               writer.element(encodedProp, attributes);
            }
         }
         writer.endElement(); // HierarchyInfo
      }
      writer.endSequence(); // "HierarchyInfo"
   }

   private Object[] getAttributes(Property prop, Hierarchy hierarchy) {
      Property longProp = longProps.get(prop.getName());
      if (longProp == null) {
         longProp = prop;
      }
      List<Object> values = new ArrayList<Object>();
      values.add("name");
      values.add(hierarchy.getUniqueName() + "." + Util.quoteMdxIdentifier(longProp.getName()));

      values.add("type");
      values.add(getXsdType(longProp));

      return values.toArray();
   }

   private String getXsdType(Property property) {
      Datatype datatype = property.getDatatype();
      return getXsdTypeByDatatype(datatype);

   }

   private String getXsdTypeByDatatype(Datatype datatype) {
      switch (datatype) {
      case UNSIGNED_INTEGER:
         return RowsetDefinition.Type.UnsignedInteger.columnType;
      case BOOLEAN:
         return RowsetDefinition.Type.Boolean.columnType;
      default:
         return RowsetDefinition.Type.String.columnType;
      }
   }

   private void axes(SaxWriter writer) throws OlapException {
      writer.startSequence("Axes", "Axis");
      // fix mondrian bug: fail to process "Cross-Join Drill Down" on level with
      // single member

      final List<CellSetAxis> axes = cellSet.getAxes();
      for (int i = 0; i < axes.size(); i++) {
         final CellSetAxis axis = axes.get(i);
         final List<Property> props = getProps(axis.getAxisMetaData());
         axis(writer, axis, props, "Axis" + i);
      }

      // //////////////////////////////////////////
      // now generate SlicerAxis information
      //
      if (omitDefaultSlicerInfo) {
         CellSetAxis slicerAxis = cellSet.getFilterAxis();
         // We always write a slicer axis. There are two 'empty' cases:
         // zero positions (which happens when the WHERE clause evalutes
         // to an empty set) or one position containing a tuple of zero
         // members (which happens when there is no WHERE clause) and we
         // need to be able to distinguish between the two.
         axis(writer, slicerAxis, getProps(slicerAxis.getAxisMetaData()), "SlicerAxis");
      } else {
         List<Hierarchy> hierarchies = slicerAxisHierarchies;
         writer.startElement("Axis", "name", "SlicerAxis");
         writer.startSequence("Tuples", "Tuple");
         writer.startSequence("Tuple", "Member");

         Map<String, Integer> memberMap = new HashMap<String, Integer>();
         Member positionMember;
         CellSetAxis slicerAxis = cellSet.getFilterAxis();
         final List<Position> slicerPositions = slicerAxis.getPositions();
         if (slicerPositions != null && slicerPositions.size() > 0) {
            final Position pos0 = slicerPositions.get(0);
            int i = 0;
            for (Member member : pos0.getMembers()) {
               memberMap.put(member.getHierarchy().getName(), i++);
            }
         }

         final List<Member> slicerMembers = slicerPositions.isEmpty() ? Collections.<Member> emptyList() : slicerPositions.get(0).getMembers();
         for (Hierarchy hierarchy : hierarchies) {
            // Find which member is on the slicer.
            // If it's not explicitly there, use the default member.
            Member member = hierarchy.getDefaultMember();
            final Integer indexPosition = memberMap.get(hierarchy.getName());
            if (indexPosition != null) {
               positionMember = slicerMembers.get(indexPosition);
            } else {
               positionMember = null;
            }
            for (Member slicerMember : slicerMembers) {
               if (slicerMember.getHierarchy().equals(hierarchy)) {
                  member = slicerMember;
                  break;
               }
            }

            if (member != null) {
               if (positionMember != null) {
                  writeMember(writer, positionMember, null, slicerPositions.get(0), indexPosition, getProps(slicerAxis.getAxisMetaData()));
               } else {
                  slicerAxis(writer, member, getProps(slicerAxis.getAxisMetaData()));
               }
            } else {
               LOGGER.warn("Can not create SlicerAxis: " + "null default member for Hierarchy " + hierarchy.getUniqueName());
            }
         }
         writer.endSequence(); // Tuple
         writer.endSequence(); // Tuples
         writer.endElement(); // Axis
      }

      //
      // //////////////////////////////////////////

      writer.endSequence(); // Axes
   }

   @SuppressWarnings("unchecked")
   private List<Property> getProps(CellSetAxisMetaData queryAxis) {
      if (queryAxis == null) {
         return defaultProps;
      }
      return CompositeList.of(defaultProps, queryAxis.getProperties());
   }

   private void axis(SaxWriter writer, CellSetAxis axis, List<Property> props, String axisName) throws OlapException {
      writer.startElement("Axis", "name", axisName);
      writer.startSequence("Tuples", "Tuple");

      List<Position> positions = axis.getPositions();
      Iterator<Position> pit = positions.iterator();
      Position prevPosition = null;
      Position position = pit.hasNext() ? pit.next() : null;
      Position nextPosition = pit.hasNext() ? pit.next() : null;
      while (position != null) {
         writer.startSequence("Tuple", "Member");
         int k = 0;

         for (Member member : position.getMembers()) {
            writeMember(writer, member, prevPosition, nextPosition, k++, props);
         }
         writer.endSequence(); // Tuple
         prevPosition = position;
         position = nextPosition;
         nextPosition = pit.hasNext() ? pit.next() : null;
      }
      writer.endSequence(); // Tuples
      writer.endElement(); // Axis
   }

   private void writeMember(SaxWriter writer, Member member, Position prevPosition, Position nextPosition, int k, List<Property> props) throws OlapException {
      writer.startElement("Member", "Hierarchy", "[" + member.getHierarchy().getName() + "]");
      int levelNo = 0;
      for (Property prop : props) {
         if (!prop.getName().equals("PARENT_UNIQUE_NAME") && !prop.getName().equals("HIERARCHY_UNIQUE_NAME")) {
            Object value;

            Property longProp = longProps.get(prop.getName());

            levelNo = (Integer) member.getPropertyValue(StandardMemberProperty.LEVEL_NUMBER);

            if (longProp == null) {
               longProp = prop;
            }
            if (longProp == StandardMemberProperty.DISPLAY_INFO) {

               // LNum is relevant to Excel drill - down behavior. The value is
               if (levelNo == 0)
                  value = 1000;
               else if (levelNo == 1)
                  value = 197608;
               else if (levelNo == 2)
                  value = 132072;
               else
                  value = 131072 + levelNo;

            } else if (longProp == StandardMemberProperty.DEPTH) {
               value = member.getDepth();
            } else {
               value = member.getPropertyValue(longProp);
            }
            if (value != null && !(levelNo == 0 && prop.getName().equals("HIERARCHY_UNIQUE_NAME"))) {
               writer.textElement(encoder.encode(prop.getName()), value);
            }
         }
      }

      writer.endElement(); // Member
   }

   private void slicerAxis(SaxWriter writer, Member member, List<Property> props) throws OlapException {
      writer.startElement("Member", "Hierarchy", member.getHierarchy().getName());
      for (Property prop : props) {
         Object value;
         Property longProp = longProps.get(prop.getName());
         if (longProp == null) {
            longProp = prop;
         }
         if (longProp == StandardMemberProperty.DISPLAY_INFO) {
            Integer childrenCard = (Integer) member.getPropertyValue(StandardMemberProperty.CHILDREN_CARDINALITY);

            int displayInfo = 0xffff & childrenCard;
            value = displayInfo;
         } else if (longProp == StandardMemberProperty.DEPTH) {
            value = member.getDepth();
         } else {
            value = member.getPropertyValue(longProp);
         }
         if (value != null) {
            writer.textElement(encoder.encode(prop.getName()), value);
         }
      }
      writer.endElement(); // Member
   }

   private void cellData(SaxWriter writer) {
      writer.startSequence("CellData", "Cell");
      final int axisCount = cellSet.getAxes().size();
      List<Integer> pos = new ArrayList<Integer>();
      for (int i = 0; i < axisCount; i++) {
         pos.add(-1);
      }
      int[] cellOrdinal = new int[] { 0 };

      int axisOrdinal = axisCount - 1;
      recurse(writer, pos, axisOrdinal, cellOrdinal);

      writer.endSequence(); // CellData
   }

   private void recurse(SaxWriter writer, List<Integer> pos, int axisOrdinal, int[] cellOrdinal) {
      if (axisOrdinal < 0) {
         emitCell(writer, pos, cellOrdinal[0]++);
      } else {
         CellSetAxis axis = cellSet.getAxes().get(axisOrdinal);
         List<Position> positions = axis.getPositions();
         for (int i = 0, n = positions.size(); i < n; i++) {
            pos.set(axisOrdinal, i);
            recurse(writer, pos, axisOrdinal - 1, cellOrdinal);
         }
      }
   }

   private void emitCell(SaxWriter writer, List<Integer> pos, int ordinal) {
      Cell cell = cellSet.getCell(pos);
      if (cell.isNull() && ordinal != 0) {
         // Ignore null cell like MS AS, except for Oth ordinal
         return;
      }

      writer.startElement("Cell", "CellOrdinal", ordinal);
      for (int i = 0; i < cellProps.size(); i++) {
         Property cellPropLong = cellPropLongs.get(i);
         Object value = cell.getPropertyValue(cellPropLong);
         if (value == null) {

            if (cellPropLong.getName().equals("LANGUAGE")) {
               value = "1033";
            } else {
               continue;
            }
         }

         final String dataType = (String) cell.getPropertyValue(StandardCellProperty.DATATYPE);
         final ValueInfo vi = new ValueInfo(dataType, value);

         // value type defined by Mondrian
         String valueType = vi.valueType;

         // return value
         String valueStr = vi.value.toString();
         
         if (!json && cellPropLong == StandardCellProperty.CELL_ORDINAL) {
           valueStr = "";
         }
         else if (!json && cellPropLong == StandardCellProperty.LANGUAGE) {
            valueStr = "1033";
         }
         // Get value for <Value/> element
         else if (!json && cellPropLong == StandardCellProperty.VALUE) {
            if (cell.isNull()) {
               // Return cell without value as in case of AS2005
               continue;
            }

            if (vi.isDecimal) {
            	//handle exception mondrian.olap.fun.MondrianEvaluationException: Expected value of type STRING; got value '1.566345018E8' (NUMERIC)
            	//This exceptions doesn't happen often (hard to reproduce). It's due to Mondrian not able to cnovert String like '1.566345018E8' to Double because of 
            	//single quote. Here fetch fetch the numeric value within single quote, and assign the value to VALUE Cell.
            	
            	//input valueStr: mondrian.olap.fun.MondrianEvaluationException: Expected value of type STRING; got value '1.566345018E8' (NUMERIC)
            	//output valueStr: 1.566345018E8
            	if(valueStr.contains("mondrian.olap.fun.MondrianEvaluationException")){
             	   int index1 = valueStr.indexOf("\'") + 1;
             	   valueStr = valueStr.substring(index1);
             	   index1 = valueStr.indexOf("\'");
             	   valueStr = valueStr.substring(0, index1);
                }
                if (valueStr.contains("E")) {
                  valueType = "xsd:double";
               } else if (valueStr.endsWith(".0") || valueStr.endsWith(".00")) {
                  valueType = "xsd:int";
                  valueStr = XmlaUtil.normalizeNumericString(valueStr, false);
               } else if (valueStr.contains(".")) {
                  valueType = "xsd:double";
                  valueStr = XmlaUtil.normalizeNumericString(valueStr, true, 3);
               } else {
                  valueType = "xsd:int";
                  valueStr = XmlaUtil.normalizeNumericString(valueStr, false);

               }

            }
            // for values other than decimal type
            else {
               valueStr = vi.value.toString();
            }
         }
         if (!json && cellPropLong == StandardCellProperty.VALUE) {
            writer.startElement(cellProps.get(i).getName(), "xsi:type", valueType);
            writer.characters(valueStr);
            writer.endElement();
         } else {
            writer.textElement(cellProps.get(i).getName(), valueStr);
         }
      }
      writer.endElement();
   }
}
 
  class MDDataSet_Tabular extends MDDataSet {
    @SuppressWarnings("unused")
   private static final Logger LOGGER = Logger.getLogger(MDDataSet_Tabular.class);

    
    private final boolean empty;
    private final int[] pos;
    private final List<Integer> posList;
    private final int axisCount;
    private int cellOrdinal;

    private static final List<Property> MemberCaptionIdArray = Collections.<Property> singletonList(StandardMemberProperty.MEMBER_CAPTION);

    private final Member[] members;
    private final ColumnHandler[] columnHandlers;

    public MDDataSet_Tabular(CellSet cellSet) {
       super(cellSet);
       final List<CellSetAxis> axes = cellSet.getAxes();
       axisCount = axes.size();
       pos = new int[axisCount];
       posList = new CustomXmlaHandler.IntList(pos);

       // Count dimensions, and deduce list of levels which appear on
       // non-COLUMNS axes.
       boolean empty = false;
       int dimensionCount = 0;
       for (int i = axes.size() - 1; i > 0; i--) {
          CellSetAxis axis = axes.get(i);
          if (axis.getPositions().size() == 0) {
             // If any axis is empty, the whole data set is empty.
             empty = true;
             continue;
          }
          dimensionCount += axis.getPositions().get(0).getMembers().size();
       }
       this.empty = empty;

       // Build a list of the lowest level used on each non-COLUMNS axis.
       Level[] levels = new Level[dimensionCount];
       List<ColumnHandler> columnHandlerList = new ArrayList<ColumnHandler>();
       int memberOrdinal = 0;
       if (!empty) {
          for (int i = axes.size() - 1; i > 0; i--) {
             final CellSetAxis axis = axes.get(i);
             final int z0 = memberOrdinal; // save ordinal so can rewind
             final List<Position> positions = axis.getPositions();
             int jj = 0;
             for (Position position : positions) {
                memberOrdinal = z0; // rewind to start
                for (Member member : position.getMembers()) {
                   if (jj == 0 || member.getLevel().getDepth() > levels[memberOrdinal].getDepth()) {
                      levels[memberOrdinal] = member.getLevel();
                   }
                   memberOrdinal++;
                }
                jj++;
             }

             // Now we know the lowest levels on this axis, add
             // properties.
             List<Property> dimProps = axis.getAxisMetaData().getProperties();
             if (dimProps.size() == 0) {
                dimProps = MemberCaptionIdArray;
             }
             for (int j = z0; j < memberOrdinal; j++) {
                Level level = levels[j];
                for (int k = 0; k <= level.getDepth(); k++) {
                   final Level level2 = level.getHierarchy().getLevels().get(k);
                   if (level2.getLevelType() == Level.Type.ALL) {
                      continue;
                   }
                   for (Property dimProp : dimProps) {
                      columnHandlerList.add(new MemberColumnHandler(dimProp, level2, j));
                   }
                }
             }
          }
       }
       this.members = new Member[memberOrdinal + 1];

       // Deduce the list of column headings.
       if (axes.size() > 0) {
          CellSetAxis columnsAxis = axes.get(0);
          for (Position position : columnsAxis.getPositions()) {
             String name = null;
             int j = 0;
             for (Member member : position.getMembers()) {
                if (j == 0) {
                   name = member.getUniqueName();
                } else {
                   name = name + "." + member.getUniqueName();
                }
                j++;
             }
             columnHandlerList.add(new CellColumnHandler(name));
          }
       }

       this.columnHandlers = columnHandlerList.toArray(new ColumnHandler[columnHandlerList.size()]);
    }

    public void metadata(SaxWriter writer) {
       writer.startElement("xs:schema", "xmlns:xs", NS_XSD, "targetNamespace", NS_XMLA_ROWSET, "xmlns", NS_XMLA_ROWSET, "xmlns:xsi", NS_XSI, "xmlns:sql",CustomXmlaHandler.NS_XML_SQL,
                "elementFormDefault", "qualified");

       { // <root>
          writer.startElement("xs:element", "name", "root");
          writer.startElement("xs:complexType");
          writer.startElement("xs:sequence");
          writer.element("xs:element", "maxOccurs", "unbounded", "minOccurs", 0, "name", "row", "type", "row");
          writer.endElement(); // xsd:sequence
          writer.endElement(); // xsd:complexType
          writer.endElement(); // xsd:element name=root
       }

       { // xsd:simpleType name="uuid"
          writer.startElement("xs:simpleType", "name", "uuid");
          writer.startElement("xs:restriction", "base", CustomXmlaHandler.XSD_STRING);
          writer.element("xs:pattern", "value", RowsetDefinition.UUID_PATTERN);
          writer.endElement(); // xsd:restriction
          writer.endElement(); // xsd:simpleType
       }

       { // xsd:complexType name="row"
          writer.startElement("xs:complexType", "name", "row");
          writer.startElement("xs:sequence");
          for (ColumnHandler columnHandler : columnHandlers) {
             columnHandler.metadata(writer);
          }
          writer.endElement(); // xsd:sequence
          writer.endElement(); // xsd:complexType
       }
       writer.endElement(); // xsd:schema
    }

    public void unparse(SaxWriter writer) throws SAXException, OlapException {
       if (empty) {
          return;
       }
       cellData(writer);
    }

    private void cellData(SaxWriter writer) throws SAXException, OlapException {
       cellOrdinal = 0;
       iterate(writer);
    }

    /**
     * Iterates over the resust writing tabular rows.
     * 
     * @param writer
     *           Writer
     * @throws org.xml.sax.SAXException
     *            on error
     */
    private void iterate(SaxWriter writer) throws SAXException, OlapException {
       switch (axisCount) {
       case 0:
          // For MDX like: SELECT FROM Sales
          emitCell(writer, cellSet.getCell(posList));
          return;
       default:
          // throw new SAXException("Too many axes: " + axisCount);
          iterate(writer, axisCount - 1, 0);
          break;
       }
    }

    private void iterate(SaxWriter writer, int axis, final int xxx) throws OlapException {
       final List<Position> positions = cellSet.getAxes().get(axis).getPositions();
       int axisLength = axis == 0 ? 1 : positions.size();

       for (int i = 0; i < axisLength; i++) {
          final Position position = positions.get(i);
          int ho = xxx;
          final List<Member> members = position.getMembers();
          for (int j = 0; j < members.size() && ho < this.members.length; j++, ho++) {
             this.members[ho] = position.getMembers().get(j);
          }

          ++cellOrdinal;
          Util.discard(cellOrdinal);

          if (axis >= 2) {
             iterate(writer, axis - 1, ho);
          } else {
             writer.startElement("row");// abrimos la fila
             pos[axis] = i; // coordenadas: fila i
             pos[0] = 0; // coordenadas (0,i): columna 0
             for (ColumnHandler columnHandler : columnHandlers) {
                if (columnHandler instanceof MemberColumnHandler) {
                   columnHandler.write(writer, null, this.members);
                } else if (columnHandler instanceof CellColumnHandler) {
                   columnHandler.write(writer, cellSet.getCell(posList), null);
                   pos[0]++;// next col.
                }
             }
             writer.endElement();// cerramos la fila
          }
       }
    }

    private void emitCell(SaxWriter writer, Cell cell) throws OlapException {
       ++cellOrdinal;
       Util.discard(cellOrdinal);

       // Ignore empty cells.
       final Object cellValue = cell.getValue();
       if (cellValue == null) {
          return;
       }

       writer.startElement("row");
       for (ColumnHandler columnHandler : columnHandlers) {
          columnHandler.write(writer, cell, members);
       }
       writer.endElement();
    }
 }

