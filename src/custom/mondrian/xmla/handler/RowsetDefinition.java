/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2011 Pentaho
// Copyright (c) 2008-2014 Open Link Financial, Inc. All Rights Reserved.
 */
package custom.mondrian.xmla.handler;

import mondrian.olap.*;
import mondrian.util.Composite;
import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
import org.olap4j.impl.ArrayNamedListImpl;
import org.olap4j.impl.Olap4jUtil;
import org.olap4j.mdx.IdentifierNode;
import org.olap4j.mdx.IdentifierSegment;
import org.olap4j.mdx.ParseTreeNode;
import org.olap4j.mdx.ParseTreeWriter;
import org.olap4j.metadata.*;
import org.olap4j.metadata.Cube;
import org.olap4j.metadata.Dimension;
import org.olap4j.metadata.Hierarchy;
import org.olap4j.metadata.Level;
import org.olap4j.metadata.Member;
import org.olap4j.metadata.Member.TreeOp;
import org.olap4j.metadata.NamedSet;
import org.olap4j.metadata.Property;
import org.olap4j.metadata.Schema;
import org.olap4j.metadata.XmlaConstants;

import custom.mondrian.xmla.exception.XmlaException;
import custom.mondrian.xmla.request.XmlaRequest;
import custom.mondrian.xmla.response.XmlaResponse;
import custom.mondrian.xmla.writer.SaxWriter;

import java.io.StringWriter;
import java.io.Writer;
import java.lang.reflect.*;
import java.sql.SQLException;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.*;

import static mondrian.olap.Util.filter;
import static custom.mondrian.xmla.handler.XmlaConstants.*;
import static custom.mondrian.xmla.handler.CustomXmlaHandler.getExtra;

/**
 * <code>RowsetDefinition</code> defines a rowset, including the columns it
 * should contain.
 * 
 * <p>
 * See "XML for Analysis Rowsets", page 38 of the XML for Analysis
 * Specification, version 1.1.
 * 
 * @author jhyde
 */

@SuppressWarnings({ "unchecked", "rawtypes" })
public enum RowsetDefinition {
   /**
    * Returns a list of XML for Analysis data sources available on the server or
    * Web Service. (For an example of how these may be published, see
    * "XML for Analysis Implementation Walkthrough" in the XML for Analysis
    * specification.)
    * 
    * http://msdn2.microsoft.com/en-us/library/ms126129(SQL.90).aspx
    * 
    * 
    * restrictions
    * 
    * Not supported
    */

   /**
    * 
    * 
    * 
    * restrictions
    * 
    * Not supported
    */
   DBSCHEMA_CATALOGS(6, "Identifies the physical attributes associated with catalogs " + "accessible from the provider.", new Column[] { DbschemaCatalogsRowset.CatalogName,
            DbschemaCatalogsRowset.Description, DbschemaCatalogsRowset.Roles, DbschemaCatalogsRowset.DateModified, }, new Column[] { DbschemaCatalogsRowset.CatalogName, }) {
      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DbschemaCatalogsRowset(request, handler);
      }
   },

   /**
    * http://msdn2.microsoft.com/en-us/library/ms126299(SQL.90).aspx
    * 
    * restrictions: TABLE_CATALOG Optional TABLE_SCHEMA Optional TABLE_NAME
    * Optional TABLE_TYPE Optional TABLE_OLAP_TYPE Optional
    * 
    * Not supported
    */
   DBSCHEMA_TABLES(9, null, new Column[] { DbschemaTablesRowset.TableCatalog, DbschemaTablesRowset.TableSchema, DbschemaTablesRowset.TableName, DbschemaTablesRowset.TableType,
            DbschemaTablesRowset.TableGuid, DbschemaTablesRowset.Description, DbschemaTablesRowset.TablePropId, DbschemaTablesRowset.DateCreated,
            DbschemaTablesRowset.DateModified, DbschemaTablesRowset.TableOlapType, }, new Column[] { DbschemaTablesRowset.TableType, DbschemaTablesRowset.TableCatalog,
            DbschemaTablesRowset.TableSchema, DbschemaTablesRowset.TableName, DbschemaTablesRowset.TableOlapType,

   }) {
      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DbschemaTablesRowset(request, handler);
      }
   },

   /**
    * 
    * 
    * 
    * restrictions
    * 
    * Not supported COLUMN_OLAP_TYPE
    */
   DBSCHEMA_COLUMNS(7, null, new Column[] { DbschemaColumnsRowset.TableCatalog, DbschemaColumnsRowset.TableSchema, DbschemaColumnsRowset.TableName,
            DbschemaColumnsRowset.ColumnName, DbschemaColumnsRowset.OrdinalPosition, DbschemaColumnsRowset.ColumnHasDefault, DbschemaColumnsRowset.ColumnFlags,
            DbschemaColumnsRowset.IsNullable, DbschemaColumnsRowset.DataType, DbschemaColumnsRowset.CharacterMaximumLength, DbschemaColumnsRowset.CharacterOctetLength,
            DbschemaColumnsRowset.NumericPrecision, DbschemaColumnsRowset.NumericScale, DbschemaColumnsRowset.ColumnOlapType }, new Column[] { DbschemaColumnsRowset.TableCatalog,
            DbschemaColumnsRowset.TableSchema, DbschemaColumnsRowset.TableName, DbschemaColumnsRowset.ColumnOlapType }) {
      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DbschemaColumnsRowset(request, handler);
      }
   },

   /**
    * 
    * 
    * 
    * restrictions
    * 
    * Not supported
    */
   DBSCHEMA_PROVIDER_TYPES(8, null, new Column[] { DbschemaProviderTypesRowset.TypeName, DbschemaProviderTypesRowset.DataType, DbschemaProviderTypesRowset.ColumnSize,
            DbschemaProviderTypesRowset.LiteralPrefix, DbschemaProviderTypesRowset.LiteralSuffix, DbschemaProviderTypesRowset.IsNullable,
            DbschemaProviderTypesRowset.CaseSensitive, DbschemaProviderTypesRowset.Searchable, DbschemaProviderTypesRowset.UnsignedAttribute,
            DbschemaProviderTypesRowset.FixedPrecScale, DbschemaProviderTypesRowset.AutoUniqueValue, DbschemaProviderTypesRowset.IsLong, DbschemaProviderTypesRowset.BestMatch, },
            new Column[] { DbschemaProviderTypesRowset.DataType, }) {
      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DbschemaProviderTypesRowset(request, handler);
      }
   },

   /**
    * http://msdn2.microsoft.com/en-us/library/ms126271(SQL.90).aspx
    * 
    * restrictions CATALOG_NAME Optional. SCHEMA_NAME Optional. CUBE_NAME
    * Optional. CUBE_TYPE (Optional) A bitmap with one of these valid values: 1
    * CUBE 2 DIMENSION Default restriction is a value of 1. BASE_CUBE_NAME
    * Optional.
    * 
    * Not supported CREATED_ON LAST_SCHEMA_UPDATE SCHEMA_UPDATED_BY
    * LAST_DATA_UPDATE DATA_UPDATED_BY ANNOTATIONS
    */
   MDSCHEMA_CUBES(12, null, new Column[] { MdschemaCubesRowset.CatalogName, MdschemaCubesRowset.SchemaName, MdschemaCubesRowset.CubeName, MdschemaCubesRowset.CubeType,
            MdschemaCubesRowset.CubeGuid, MdschemaCubesRowset.CreatedOn, MdschemaCubesRowset.LastSchemaUpdate, MdschemaCubesRowset.SchemaUpdatedBy,
            MdschemaCubesRowset.LastDataUpdate, MdschemaCubesRowset.DataUpdatedBy, MdschemaCubesRowset.IsDrillthroughEnabled, MdschemaCubesRowset.IsWriteEnabled,
            MdschemaCubesRowset.IsLinkable, MdschemaCubesRowset.IsSqlEnabled, MdschemaCubesRowset.CubeCaption, MdschemaCubesRowset.CubeSource,MdschemaCubesRowset.BaseCubeName, 
            MdschemaCubesRowset.PreferedQueryPatterns, MdschemaCubesRowset.Description, MdschemaCubesRowset.Dimensions, MdschemaCubesRowset.Sets, MdschemaCubesRowset.Measures },
            new Column[] { MdschemaCubesRowset.CatalogName, MdschemaCubesRowset.SchemaName, MdschemaCubesRowset.CubeName, MdschemaCubesRowset.CubeSource,  MdschemaCubesRowset.BaseCubeName,
                    MdschemaCubesRowset.PreferedQueryPatterns, }) {
      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new MdschemaCubesRowset(request, handler);
      }
   },

   /**
    * http://msdn2.microsoft.com/en-us/library/ms126180(SQL.90).aspx
    * http://msdn2.microsoft.com/en-us/library/ms126180.aspx
    * 
    * restrictions CATALOG_NAME Optional. SCHEMA_NAME Optional. CUBE_NAME
    * Optional. DIMENSION_NAME Optional. DIMENSION_UNIQUE_NAME Optional.
    * CUBE_SOURCE (Optional) A bitmap with one of the following valid values: 1
    * CUBE 2 DIMENSION Default restriction is a value of 1.
    * 
    * DIMENSION_VISIBILITY (Optional) A bitmap with one of the following valid
    * values: 1 Visible 2 Not visible Default restriction is a value of 1.
    */
   MDSCHEMA_DIMENSIONS(13, null, new Column[] { MdschemaDimensionsRowset.CatalogName, MdschemaDimensionsRowset.SchemaName, MdschemaDimensionsRowset.CubeName,
            MdschemaDimensionsRowset.DimensionName, MdschemaDimensionsRowset.DimensionUniqueName, MdschemaDimensionsRowset.DimensionGuid,
            MdschemaDimensionsRowset.DimensionCaption, MdschemaDimensionsRowset.DimensionOrdinal, MdschemaDimensionsRowset.DimensionType,
            MdschemaDimensionsRowset.DimensionCardinality, MdschemaDimensionsRowset.DefaultHierarchy, MdschemaDimensionsRowset.Description, MdschemaDimensionsRowset.IsVirtual,
            MdschemaDimensionsRowset.IsReadWrite, MdschemaDimensionsRowset.DimensionUniqueSettings, MdschemaDimensionsRowset.DimensionMasterUniqueName, MdschemaDimensionsRowset.CubeSource,
            MdschemaDimensionsRowset.DimensionVisibility, MdschemaDimensionsRowset.Hierarchies, 

   }, new Column[] { MdschemaDimensionsRowset.CatalogName, MdschemaDimensionsRowset.SchemaName, MdschemaDimensionsRowset.CubeName, MdschemaDimensionsRowset.DimensionName,
            MdschemaDimensionsRowset.CubeSource, }) {
      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new MdschemaDimensionsRowset(request, handler);
      }
   },

   /**
    * http://msdn2.microsoft.com/en-us/library/ms126062(SQL.90).aspx
    * 
    * restrictions CATALOG_NAME Optional. SCHEMA_NAME Optional. CUBE_NAME
    * Optional. DIMENSION_UNIQUE_NAME Optional. HIERARCHY_NAME Optional.
    * HIERARCHY_UNIQUE_NAME Optional. HIERARCHY_ORIGIN (Optional) A default
    * restriction is in effect on MD_USER_DEFINED and MD_SYSTEM_ENABLED.
    * CUBE_SOURCE (Optional) A bitmap with one of the following valid values: 1
    * CUBE 2 DIMENSION Default restriction is a value of 1. HIERARCHY_VISIBILITY
    * (Optional) A bitmap with one of the following valid values: 1 Visible 2
    * Not visible Default restriction is a value of 1.
    * 
    * Not supported HIERARCHY_ORIGIN HIERARCHY_DISPLAY_FOLDER INSTANCE_SELECTION
    */
   MDSCHEMA_HIERARCHIES(15, null, new Column[] { MdschemaHierarchiesRowset.CatalogName, MdschemaHierarchiesRowset.SchemaName, MdschemaHierarchiesRowset.CubeName,
            MdschemaHierarchiesRowset.DimensionUniqueName, MdschemaHierarchiesRowset.HierarchyName, MdschemaHierarchiesRowset.HierarchyUniqueName,
            MdschemaHierarchiesRowset.HierarchyGuid, MdschemaHierarchiesRowset.HierarchyCaption, MdschemaHierarchiesRowset.DimensionType,
            MdschemaHierarchiesRowset.HierarchyCardinality, MdschemaHierarchiesRowset.DefaultMember, MdschemaHierarchiesRowset.AllMember, MdschemaHierarchiesRowset.Description,
            MdschemaHierarchiesRowset.Structure, MdschemaHierarchiesRowset.IsVirtual, MdschemaHierarchiesRowset.IsReadWrite, MdschemaHierarchiesRowset.DimensionUniqueSettings,
            MdschemaHierarchiesRowset.DimensionMasterUniqueName, MdschemaHierarchiesRowset.DimensionIsVisible, MdschemaHierarchiesRowset.HierarchyOrdinal,
            MdschemaHierarchiesRowset.DimensionIsShared, MdschemaHierarchiesRowset.HierarchyOrigin,
            MdschemaHierarchiesRowset.CubeSource,MdschemaHierarchiesRowset.HierarchyIsVisible, MdschemaHierarchiesRowset.HierarchyVisibility,

             MdschemaHierarchiesRowset.HierarchyDisplayFolder, MdschemaHierarchiesRowset.InstanceSelection,
            MdschemaHierarchiesRowset.GroupingBehaviors, MdschemaHierarchiesRowset.StructureType, }, new Column[] { MdschemaHierarchiesRowset.CatalogName,
            MdschemaHierarchiesRowset.SchemaName, MdschemaHierarchiesRowset.CubeName, MdschemaHierarchiesRowset.DimensionUniqueName, MdschemaHierarchiesRowset.HierarchyName,
            MdschemaHierarchiesRowset.HierarchyVisibility, MdschemaHierarchiesRowset.CubeSource }) {
      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new MdschemaHierarchiesRowset(request, handler);
      }
   },

   /**
    * http://msdn2.microsoft.com/en-us/library/ms126038(SQL.90).aspx
    * 
    * restriction CATALOG_NAME Optional. SCHEMA_NAME Optional. CUBE_NAME
    * Optional. DIMENSION_UNIQUE_NAME Optional. HIERARCHY_UNIQUE_NAME Optional.
    * LEVEL_NAME Optional. LEVEL_UNIQUE_NAME Optional. LEVEL_ORIGIN (Optional) A
    * default restriction is in effect on MD_USER_DEFINED and MD_SYSTEM_ENABLED
    * CUBE_SOURCE (Optional) A bitmap with one of the following valid values: 1
    * CUBE 2 DIMENSION Default restriction is a value of 1. LEVEL_VISIBILITY
    * (Optional) A bitmap with one of the following values: 1 Visible 2 Not
    * visible Default restriction is a value of 1.
    * 
    * Not supported CUSTOM_ROLLUP_SETTINGS LEVEL_UNIQUE_SETTINGS
    * LEVEL_ORDERING_PROPERTY LEVEL_DBTYPE LEVEL_MASTER_UNIQUE_NAME
    * LEVEL_NAME_SQL_COLUMN_NAME Customers:(All)!NAME LEVEL_KEY_SQL_COLUMN_NAME
    * Customers:(All)!KEY LEVEL_UNIQUE_NAME_SQL_COLUMN_NAME
    * Customers:(All)!UNIQUE_NAME LEVEL_ATTRIBUTE_HIERARCHY_NAME
    * LEVEL_KEY_CARDINALITY LEVEL_ORIGIN
    * 
    */
   MDSCHEMA_LEVELS(16, null, new Column[] { MdschemaLevelsRowset.CatalogName, MdschemaLevelsRowset.SchemaName, MdschemaLevelsRowset.CubeName,
            MdschemaLevelsRowset.DimensionUniqueName, MdschemaLevelsRowset.HierarchyUniqueName, MdschemaLevelsRowset.LevelName, MdschemaLevelsRowset.LevelUniqueName,
            MdschemaLevelsRowset.LevelOrigin, 
            MdschemaLevelsRowset.LevelGuid, MdschemaLevelsRowset.LevelCaption, MdschemaLevelsRowset.LevelNumber, MdschemaLevelsRowset.LevelCardinality,
            MdschemaLevelsRowset.LevelType, MdschemaLevelsRowset.CustomRollupSettings, MdschemaLevelsRowset.LevelUniqueSettings, 
            MdschemaLevelsRowset.Description,MdschemaLevelsRowset.CubeSource,MdschemaLevelsRowset.LevelVisibility,

            MdschemaLevelsRowset.LevelDbtype, MdschemaLevelsRowset.LevelKeyCardinality, MdschemaLevelsRowset.LevelNameSqlColumnName,
            MdschemaLevelsRowset.LevelKeySqlColumnName, MdschemaLevelsRowset.LevelUniqueNameSqlColumnName, MdschemaLevelsRowset.LevelAttributeHierarchyName,
            MdschemaLevelsRowset.LevelOrderingProperty, 

   }, new Column[] { MdschemaLevelsRowset.CatalogName, MdschemaLevelsRowset.SchemaName, MdschemaLevelsRowset.CubeName, MdschemaLevelsRowset.DimensionUniqueName,
            MdschemaLevelsRowset.HierarchyUniqueName, MdschemaLevelsRowset.LevelNumber, MdschemaLevelsRowset.CubeSource

   // MdschemaLevelsRowset.LevelDbtype,
   // MdschemaLevelsRowset.LevelKeyCardinality,
   // MdschemaLevelsRowset.LevelOrigin,

            }) {
      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new MdschemaLevelsRowset(request, handler);
      }
   },

   /**
    * http://msdn2.microsoft.com/en-us/library/ms126250(SQL.90).aspx
    * 
    * restrictions CATALOG_NAME Optional. SCHEMA_NAME Optional. CUBE_NAME
    * Optional. MEASURE_NAME Optional. MEASURE_UNIQUE_NAME Optional. CUBE_SOURCE
    * (Optional) A bitmap with one of the following valid values: 1 CUBE 2
    * DIMENSION Default restriction is a value of 1. MEASURE_VISIBILITY
    * (Optional) A bitmap with one of the following valid values: 1 Visible 2
    * Not Visible Default restriction is a value of 1.
    * 
    * Not supported MEASURE_GUID NUMERIC_PRECISION NUMERIC_SCALE MEASURE_UNITS
    * EXPRESSION MEASURE_NAME_SQL_COLUMN_NAME MEASURE_UNQUALIFIED_CAPTION
    * MEASUREGROUP_NAME MEASURE_DISPLAY_FOLDER DEFAULT_FORMAT_STRING
    */
   MDSCHEMA_MEASURES(17, null, new Column[] { MdschemaMeasuresRowset.CatalogName, MdschemaMeasuresRowset.SchemaName, MdschemaMeasuresRowset.CubeName,
            MdschemaMeasuresRowset.MeasureName, MdschemaMeasuresRowset.MeasureUniqueName, MdschemaMeasuresRowset.MeasureCaption, MdschemaMeasuresRowset.MeasureGuid,
            MdschemaMeasuresRowset.MeasureAggregator, MdschemaMeasuresRowset.DataType, MdschemaMeasuresRowset.NumericPrecision, MdschemaMeasuresRowset.NumericScale,
            MdschemaMeasuresRowset.MeasureUnits, MdschemaMeasuresRowset.Description, MdschemaMeasuresRowset.Expression, MdschemaMeasuresRowset.MeasureIsVisible,
            MdschemaMeasuresRowset.LevelsList, MdschemaMeasuresRowset.MeasureNameSql, MdschemaMeasuresRowset.MeasureUnqualifiedCaption, MdschemaMeasuresRowset.MeasureGroupName,
            MdschemaMeasuresRowset.MeasureDisplayFolder, MdschemaMeasuresRowset.FormatString, MdschemaMeasuresRowset.CubeSource, MdschemaMeasuresRowset.MeasureVisibility, },
            new Column[] { MdschemaMeasuresRowset.CatalogName, MdschemaMeasuresRowset.SchemaName, MdschemaMeasuresRowset.CubeName, MdschemaMeasuresRowset.MeasureName,
                     MdschemaMeasuresRowset.NumericScale, MdschemaMeasuresRowset.MeasureIsVisible, MdschemaMeasuresRowset.MeasureNameSql, MdschemaMeasuresRowset.MeasureGroupName,
                     MdschemaMeasuresRowset.CubeSource, MdschemaMeasuresRowset.MeasureVisibility }) {
      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new MdschemaMeasuresRowset(request, handler);
      }
   },

   /**
    * http://msdn2.microsoft.com/en-us/library/ms126309(SQL.90).aspx
    * 
    * restrictions CATALOG_NAME Mandatory SCHEMA_NAME Optional CUBE_NAME
    * Optional DIMENSION_UNIQUE_NAME Optional HIERARCHY_UNIQUE_NAME Optional
    * LEVEL_UNIQUE_NAME Optional
    * 
    * MEMBER_UNIQUE_NAME Optional PROPERTY_NAME Optional PROPERTY_TYPE Optional
    * PROPERTY_CONTENT_TYPE (Optional) A default restriction is in place on
    * MDPROP_MEMBER OR MDPROP_CELL. PROPERTY_ORIGIN (Optional) A default
    * restriction is in place on MD_USER_DEFINED OR MD_SYSTEM_ENABLED
    * CUBE_SOURCE (Optional) A bitmap with one of the following valid values: 1
    * CUBE 2 DIMENSION Default restriction is a value of 1. PROPERTY_VISIBILITY
    * (Optional) A bitmap with one of the following valid values: 1 Visible 2
    * Not visible Default restriction is a value of 1.
    * 
    * Not supported PROPERTY_ORIGIN CUBE_SOURCE PROPERTY_VISIBILITY
    * CHARACTER_MAXIMUM_LENGTH CHARACTER_OCTET_LENGTH NUMERIC_PRECISION
    * NUMERIC_SCALE DESCRIPTION SQL_COLUMN_NAME LANGUAGE
    * PROPERTY_ATTRIBUTE_HIERARCHY_NAME PROPERTY_CARDINALITY MIME_TYPE
    * PROPERTY_IS_VISIBLE
    */
   MDSCHEMA_PROPERTIES(19, null, new Column[] { 
            MdschemaPropertiesRowset.CatalogName, 
            MdschemaPropertiesRowset.SchemaName, 
            MdschemaPropertiesRowset.CubeName,
            MdschemaPropertiesRowset.DimensionUniqueName, 
            MdschemaPropertiesRowset.HierarchyUniqueName, 
            MdschemaPropertiesRowset.LevelUniqueName,
            MdschemaPropertiesRowset.MemberUniqueName, 
            MdschemaPropertiesRowset.PropertyName, 
            MdschemaPropertiesRowset.PropertyType, 
            MdschemaPropertiesRowset.PropertyContentType,
            MdschemaPropertiesRowset.PropertyOrigin, 
            MdschemaPropertiesRowset.CubeSource, 
            MdschemaPropertiesRowset.PropertyVisibility, 
            MdschemaPropertiesRowset.Description, 
            MdschemaPropertiesRowset.PropertyCaption,
            MdschemaPropertiesRowset.DataType, 
            MdschemaPropertiesRowset.CharacterMaximumLength, // CHARACTER_MAXIMUM_LENGTH
            MdschemaPropertiesRowset.CharacterOctetLength,// CHARACTER_OCTET_LENGTH
            MdschemaPropertiesRowset.NumericPrecision,// NUMERIC_PRECISION
            MdschemaPropertiesRowset.NumericScale,// NUMERIC_SCALE
            MdschemaPropertiesRowset.SqlColumnName, // SQL_COLUMN_NAME
            MdschemaPropertiesRowset.Language, MdschemaPropertiesRowset.PropertyAttributeHierarchyName,
            // MdschemaPropertiesRowset.CubeSource,
            MdschemaPropertiesRowset.PropertyCardinality, MdschemaPropertiesRowset.MimeType,

   }, null /* not sorted */) {
      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new MdschemaPropertiesRowset(request, handler);
      }
   },

   /**
    * 
    * http://msdn2.microsoft.com/es-es/library/ms126046.aspx
    * 
    * 
    * restrictions CATALOG_NAME Optional. SCHEMA_NAME Optional. CUBE_NAME
    * Optional. DIMENSION_UNIQUE_NAME Optional. HIERARCHY_UNIQUE_NAME Optional.
    * LEVEL_UNIQUE_NAME Optional. LEVEL_NUMBER Optional. MEMBER_NAME Optional.
    * MEMBER_UNIQUE_NAME Optional. MEMBER_CAPTION Optional. MEMBER_TYPE
    * Optional. TREE_OP (Optional) Only applies to a single member:
    * MDTREEOP_ANCESTORS (0x20) returns all of the ancestors. MDTREEOP_CHILDREN
    * (0x01) returns only the immediate children. MDTREEOP_SIBLINGS (0x02)
    * returns members on the same level. MDTREEOP_PARENT (0x04) returns only the
    * immediate parent. MDTREEOP_SELF (0x08) returns itself in the list of
    * returned rows. MDTREEOP_DESCENDANTS (0x10) returns all of the descendants.
    * CUBE_SOURCE (Optional) A bitmap with one of the following valid values: 1
    * CUBE 2 DIMENSION Default restriction is a value of 1.
    * 
    * Not supported
    */
   MDSCHEMA_MEMBERS(18, null, new Column[] { MdschemaMembersRowset.CatalogName, MdschemaMembersRowset.SchemaName, MdschemaMembersRowset.CubeName,
            MdschemaMembersRowset.DimensionUniqueName, MdschemaMembersRowset.HierarchyUniqueName, MdschemaMembersRowset.LevelUniqueName, MdschemaMembersRowset.LevelNumber,
            MdschemaMembersRowset.MemberOrdinal, MdschemaMembersRowset.MemberName, MdschemaMembersRowset.MemberUniqueName,  MdschemaMembersRowset.MemberCaption, MdschemaMembersRowset.MemberType,
            MdschemaMembersRowset.MemberGuid, MdschemaMembersRowset.ChildrenCardinality, MdschemaMembersRowset.ParentLevel,
            MdschemaMembersRowset.ParentUniqueName, MdschemaMembersRowset.ParentCount, MdschemaMembersRowset.TreeOp_, MdschemaMembersRowset.CubeSource,MdschemaMembersRowset.Scope,
            MdschemaMembersRowset.Depth, MdschemaMembersRowset.MemberKey, MdschemaMembersRowset.IsPlaceHolderMember, MdschemaMembersRowset.IsDatamember, }, new Column[] {
            MdschemaMembersRowset.CatalogName, MdschemaMembersRowset.SchemaName, MdschemaMembersRowset.CubeName, MdschemaMembersRowset.DimensionUniqueName,
            MdschemaMembersRowset.HierarchyUniqueName, MdschemaMembersRowset.LevelUniqueName, MdschemaMembersRowset.LevelNumber, MdschemaMembersRowset.MemberOrdinal,
            MdschemaMembersRowset.IsPlaceHolderMember, MdschemaMembersRowset.IsDatamember, MdschemaMembersRowset.Scope }) {
      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new MdschemaMembersRowset(request, handler);
      }
   },

   /**
    * http://msdn2.microsoft.com/en-us/library/ms126257(SQL.90).aspx
    * 
    * restrictions LIBRARY_NAME Optional. INTERFACE_NAME Optional. FUNCTION_NAME
    * Optional. ORIGIN Optional.
    * 
    * Not supported DLL_NAME Optional HELP_FILE Optional HELP_CONTEXT Optional -
    * SQL Server xml schema says that this must be present OBJECT Optional
    * CAPTION The display caption for the function.
    */
   MDSCHEMA_FUNCTIONS(14, null, new Column[] {MdschemaFunctionsRowset.LibraryName, MdschemaFunctionsRowset.Description, MdschemaFunctionsRowset.ParameterList,
            MdschemaFunctionsRowset.ReturnType,  MdschemaFunctionsRowset.InterfaceName, MdschemaFunctionsRowset.FunctionName, MdschemaFunctionsRowset.Origin,
            MdschemaFunctionsRowset.Caption, }, new Column[] { MdschemaFunctionsRowset.LibraryName, MdschemaFunctionsRowset.InterfaceName, MdschemaFunctionsRowset.FunctionName,
            MdschemaFunctionsRowset.Origin, }) {
      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new MdschemaFunctionsRowset(request, handler);
      }
   },

   /**
    * http://msdn2.microsoft.com/en-us/library/ms126032(SQL.90).aspx
    * 
    * restrictions CATALOG_NAME Optional SCHEMA_NAME Optional CUBE_NAME
    * Mandatory ACTION_NAME Optional ACTION_TYPE Optional COORDINATE Mandatory
    * COORDINATE_TYPE Mandatory INVOCATION (Optional) The INVOCATION restriction
    * column defaults to the value of MDACTION_INVOCATION_INTERACTIVE. To
    * retrieve all actions, use the MDACTION_INVOCATION_ALL value in the
    * INVOCATION restriction column. CUBE_SOURCE (Optional) A bitmap with one of
    * the following valid values:
    * 
    * 1 CUBE 2 DIMENSION
    * 
    * Default restriction is a value of 1.
    * 
    * Not supported
    */
   MDSCHEMA_ACTIONS(11, null, new Column[] { MdschemaActionsRowset.CatalogName, MdschemaActionsRowset.SchemaName, MdschemaActionsRowset.CubeName, MdschemaActionsRowset.ActionName,
            MdschemaActionsRowset.ActionType, MdschemaActionsRowset.Coordinate, MdschemaActionsRowset.CoordinateType, MdschemaActionsRowset.Invocation,
            MdschemaActionsRowset.CubeSource }, new Column[] {
            // Spec says sort on CATALOG_NAME, SCHEMA_NAME, CUBE_NAME,
            // ACTION_NAME.
            MdschemaActionsRowset.CatalogName, MdschemaActionsRowset.SchemaName, MdschemaActionsRowset.CubeName, MdschemaActionsRowset.ActionName,
            MdschemaActionsRowset.ActionType, MdschemaActionsRowset.Invocation, MdschemaActionsRowset.CubeSource }) {
      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new MdschemaActionsRowset(request, handler);
      }
   },

   /**
    * http://msdn2.microsoft.com/en-us/library/ms126290(SQL.90).aspx
    * 
    * restrictions CATALOG_NAME Optional. SCHEMA_NAME Optional. CUBE_NAME
    * Optional. SET_NAME Optional. SCOPE Optional. HIERARCHY_UNIQUE_NAME
    * Optional. CUBE_SOURCE Optional. Note: Only one hierarchy can be included,
    * and only those named sets whose hierarchies exactly match the restriction
    * are returned.
    * 
    * Not supported EXPRESSION DIMENSIONS SET_DISPLAY_FOLDER
    */
   MDSCHEMA_SETS(20, null, new Column[] { MdschemaSetsRowset.CatalogName, MdschemaSetsRowset.SchemaName, MdschemaSetsRowset.CubeName, MdschemaSetsRowset.SetName,
            MdschemaSetsRowset.Scope, MdschemaSetsRowset.Description, MdschemaSetsRowset.Expression, MdschemaSetsRowset.Dimensions, MdschemaSetsRowset.SetDisplayFolder,
            MdschemaSetsRowset.HierarchyUniqueName, MdschemaSetsRowset.CubeSource, MdschemaSetsRowset.SetEvaluationContext  },

   new Column[] { MdschemaSetsRowset.CatalogName, MdschemaSetsRowset.SchemaName, MdschemaSetsRowset.CubeName, MdschemaSetsRowset.CubeSource,
            MdschemaSetsRowset.SetEvaluationContext }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new MdschemaSetsRowset(request, handler);
      }
   },

   /**
    * restrictions
    * 
    * Not supported
    */
   DISCOVER_INSTANCES(15, "", new Column[] { DiscoverInstancesRowset.InstanceName }, null /*
                                                                                           * not
                                                                                           * sorted
                                                                                           */) {
      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DiscoverLiteralsRowset(request, handler);
      }
   },

   MDSCHEMA_KPIS(20, null, new Column[] { MdschemaKpisRowset.CatalogName, MdschemaKpisRowset.SchemaName, MdschemaKpisRowset.CubeName, MdschemaKpisRowset.KpiName,
            MdschemaKpisRowset.CubeSource, MdschemaKpisRowset.Scope },

   new Column[] { MdschemaKpisRowset.CatalogName, MdschemaKpisRowset.SchemaName, MdschemaKpisRowset.CubeName, MdschemaKpisRowset.KpiName, MdschemaKpisRowset.CubeSource,
            MdschemaKpisRowset.Scope }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new MdschemaKpisRowset(request, handler);
      }
   },

   MDSCHEMA_MEASUREGROUPS(20, null, new Column[] { MdschemaMeasureGroupRowset.CatalogName, MdschemaMeasureGroupRowset.SchemaName, MdschemaMeasureGroupRowset.CubeName,
            MdschemaMeasureGroupRowset.MeasureGroupName,

            MdschemaMeasureGroupRowset.Description, MdschemaMeasureGroupRowset.IsWriteEnabled, MdschemaMeasureGroupRowset.MeasureGroupCaption,
            MdschemaMeasureGroupRowset.CubeSource },

   new Column[] { MdschemaMeasureGroupRowset.CatalogName, MdschemaMeasureGroupRowset.SchemaName, MdschemaMeasureGroupRowset.CubeName,
            MdschemaMeasureGroupRowset.MeasureGroupCaption, MdschemaMeasureGroupRowset.CubeSource }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new MdschemaMeasureGroupRowset(request, handler);
      }
   },

   MDSCHEMA_MEASUREGROUP_DIMENSIONS(20, null, new Column[] { MdschemaMeasureGroupDimensionRowset.CatalogName, MdschemaMeasureGroupDimensionRowset.SchemaName,
            MdschemaMeasureGroupDimensionRowset.CubeName, MdschemaMeasureGroupDimensionRowset.MeasureGroupName, MdschemaMeasureGroupDimensionRowset.MeasureGroupCardinality,
            MdschemaMeasureGroupDimensionRowset.DimensionUniqueName, MdschemaMeasureGroupDimensionRowset.DimensionCardinality,
            MdschemaMeasureGroupDimensionRowset.DimensionVisibility, MdschemaMeasureGroupDimensionRowset.DimensionISFactDimension,
            MdschemaMeasureGroupDimensionRowset.DimensionPath, MdschemaMeasureGroupDimensionRowset.DimensionGranularity, },

   new Column[] { MdschemaMeasureGroupDimensionRowset.CatalogName, MdschemaMeasureGroupDimensionRowset.SchemaName, MdschemaMeasureGroupDimensionRowset.CubeName,
            MdschemaMeasureGroupDimensionRowset.MeasureGroupName, MdschemaMeasureGroupDimensionRowset.MeasureGroupCardinality,
            MdschemaMeasureGroupDimensionRowset.DimensionUniqueName, MdschemaMeasureGroupDimensionRowset.DimensionCardinality,
            MdschemaMeasureGroupDimensionRowset.DimensionVisibility, MdschemaMeasureGroupDimensionRowset.DimensionISFactDimension,
            MdschemaMeasureGroupDimensionRowset.DimensionPath, MdschemaMeasureGroupDimensionRowset.DimensionGranularity, }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new MdschemaMeasureGroupDimensionRowset(request, handler);
      }
   },

   MDSCHEMA_INPUT_DATASOURCES(20, null, new Column[] { MdschemaInputDataSourcesSchema.CatalogName, MdschemaInputDataSourcesSchema.SchemaName,
            MdschemaInputDataSourcesSchema.DataSourceName, MdschemaInputDataSourcesSchema.DataSourceType }, new Column[] { MdschemaInputDataSourcesSchema.CatalogName,
            MdschemaInputDataSourcesSchema.SchemaName, MdschemaInputDataSourcesSchema.DataSourceName, MdschemaInputDataSourcesSchema.DataSourceType }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new MdschemaInputDataSourcesSchema(request, handler);
      }
   },

   // DMSCHEMA_MINING_SERVICES
   DMSCHEMA_MINING_SERVICES(20, null, new Column[] { DmschemaMiningServices.ServiceName, DmschemaMiningServices.ServiceTypeId }, new Column[] { DmschemaMiningServices.ServiceName,
            DmschemaMiningServices.ServiceTypeId }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DmschemaMiningServices(request, handler);
      }
   },

   // DMSCHEMA_MINING_SERVICE_PARAMETERS
   DMSCHEMA_MINING_SERVICE_PARAMETERS(20, null, new Column[] { DmschemaMiningServiceParametersRowset.ServiceName, DmschemaMiningServiceParametersRowset.ParameterName },
            new Column[] { DmschemaMiningServiceParametersRowset.ServiceName, DmschemaMiningServiceParametersRowset.ParameterName }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DmschemaMiningServiceParametersRowset(request, handler);
      }
   },

   // DMSCHEMA_MINING_FUNCTIONS
   DMSCHEMA_MINING_FUNCTIONS(20, null, new Column[] { DmschemaMiningFunctionsRowset.ServiceName, DmschemaMiningFunctionsRowset.FunctionName }, new Column[] {
            DmschemaMiningFunctionsRowset.FunctionName, DmschemaMiningFunctionsRowset.FunctionName }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DmschemaMiningFunctionsRowset(request, handler);
      }
   },

   // DMSCHEMA_MINING_MODEL_CONTENT

   DMSCHEMA_MINING_MODEL_CONTENT(20, null, new Column[] { DmschemaMiningModelContentRowset.ModelCatalog, DmschemaMiningModelContentRowset.ModelSchema,
            DmschemaMiningModelContentRowset.ModelName, DmschemaMiningModelContentRowset.AttributeName, DmschemaMiningModelContentRowset.NodeName,
            DmschemaMiningModelContentRowset.NodeUniqueName, DmschemaMiningModelContentRowset.NodeType, DmschemaMiningModelContentRowset.NodeGuid,
            DmschemaMiningModelContentRowset.NodeCaption, DmschemaMiningModelContentRowset.TreeOperation }, new Column[] { DmschemaMiningModelContentRowset.ModelCatalog,
            DmschemaMiningModelContentRowset.ModelSchema, DmschemaMiningModelContentRowset.ModelName, DmschemaMiningModelContentRowset.AttributeName,
            DmschemaMiningModelContentRowset.NodeName, DmschemaMiningModelContentRowset.NodeUniqueName, DmschemaMiningModelContentRowset.NodeType,
            DmschemaMiningModelContentRowset.NodeGuid, DmschemaMiningModelContentRowset.NodeCaption, DmschemaMiningModelContentRowset.TreeOperation }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DmschemaMiningFunctionsRowset(request, handler);
      }
   },
   // DMSCHEMA_MINING_MODEL_XML

   DMSCHEMA_MINING_MODEL_XML(20, null, new Column[] { DmschemaMiningModelXmlRowset.ModelCatalog, DmschemaMiningModelXmlRowset.ModelSchema, DmschemaMiningModelXmlRowset.ModelName,
            DmschemaMiningModelXmlRowset.ModelType }, new Column[] { DmschemaMiningModelXmlRowset.ModelCatalog, DmschemaMiningModelXmlRowset.ModelSchema,
            DmschemaMiningModelXmlRowset.ModelName, DmschemaMiningModelXmlRowset.ModelType }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DmschemaMiningModelXmlRowset(request, handler);
      }
   },
   // DMSCHEMA_MINING_MODEL_CONTENT_PMML
   DMSCHEMA_MINING_MODEL_CONTENT_PMML(20, null, new Column[] { DbschemaMiningCodelContentPmmlRowset.ModelCatalog, DbschemaMiningCodelContentPmmlRowset.ModelSchema,
            DbschemaMiningCodelContentPmmlRowset.ModelName, DbschemaMiningCodelContentPmmlRowset.ModelType }, new Column[] { DbschemaMiningCodelContentPmmlRowset.ModelCatalog,
            DbschemaMiningCodelContentPmmlRowset.ModelSchema, DbschemaMiningCodelContentPmmlRowset.ModelName, DbschemaMiningCodelContentPmmlRowset.ModelType }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DmschemaMiningModelXmlRowset(request, handler);
      }
   },

   // DMSCHEMA_MINING_MODELS
   DMSCHEMA_MINING_MODELS(20, null, new Column[] { DbschemaMiningModelsRowset.ModelCatalog, DbschemaMiningModelsRowset.ModelSchema, DbschemaMiningModelsRowset.ModelName,
            DbschemaMiningModelsRowset.ModelType, DbschemaMiningModelsRowset.ServiceName, DbschemaMiningModelsRowset.ServiceTypeId, DbschemaMiningModelsRowset.MiningStructure },
            new Column[] { DbschemaMiningModelsRowset.ModelCatalog, DbschemaMiningModelsRowset.ModelSchema, DbschemaMiningModelsRowset.ModelName,
                     DbschemaMiningModelsRowset.ModelType, DbschemaMiningModelsRowset.ServiceName, DbschemaMiningModelsRowset.ServiceTypeId,
                     DbschemaMiningModelsRowset.MiningStructure }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DmschemaMiningModelXmlRowset(request, handler);
      }
   },

   // DMSCHEMA_MINING_COLUMNS

   DMSCHEMA_MINING_COLUMNS(20, null, new Column[] { DbschemaMiningColumnsRowset.ModelCatalog, DbschemaMiningColumnsRowset.ModelSchema, DbschemaMiningColumnsRowset.ModelName,
            DbschemaMiningColumnsRowset.ColumnName }, new Column[] { DbschemaMiningColumnsRowset.ModelCatalog, DbschemaMiningColumnsRowset.ModelSchema,
            DbschemaMiningColumnsRowset.ModelName, DbschemaMiningColumnsRowset.ColumnName }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DbschemaMiningColumnsRowset(request, handler);
      }
   },

   // DMSCHEMA_MINING_STRUCTURES

   DMSCHEMA_MINING_STRUCTURES(20, null, new Column[] { DbschemaMiningStructuresRowset.StructureCatalog, DbschemaMiningStructuresRowset.StructureSchema,
            DbschemaMiningStructuresRowset.StructureName }, new Column[] { DbschemaMiningStructuresRowset.StructureCatalog, DbschemaMiningStructuresRowset.StructureSchema,
            DbschemaMiningStructuresRowset.StructureName }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DbschemaMiningColumnsRowset(request, handler);
      }
   },

   // DMSCHEMA_MINING_STRUCTURE_COLUMNS

   DMSCHEMA_MINING_STRUCTURE_COLUMNS(20, null, new Column[] { DbschemaMiningStructuresColumnsRowset.StructureCatalog, DbschemaMiningStructuresColumnsRowset.StructureSchema,
            DbschemaMiningStructuresColumnsRowset.StructureName, DbschemaMiningStructuresColumnsRowset.ColumnName }, new Column[] {
            DbschemaMiningStructuresColumnsRowset.StructureCatalog, DbschemaMiningStructuresColumnsRowset.StructureSchema, DbschemaMiningStructuresColumnsRowset.StructureName,
            DbschemaMiningStructuresColumnsRowset.ColumnName }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DbschemaMiningStructuresColumnsRowset(request, handler);
      }
   },

   DISCOVER_DATASOURCES(0, "Returns a list of XML for Analysis data sources available on the " + "server or Web Service.", new Column[] { DiscoverDatasourcesRowset.DataSourceName,
            DiscoverDatasourcesRowset.DataSourceDescription, DiscoverDatasourcesRowset.URL, DiscoverDatasourcesRowset.DataSourceInfo, DiscoverDatasourcesRowset.ProviderName,
            DiscoverDatasourcesRowset.ProviderType, DiscoverDatasourcesRowset.AuthenticationMode, },
   // XMLA does not specify a sort order, but olap4j does.
            new Column[] { DiscoverDatasourcesRowset.DataSourceName, }) {
      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DiscoverDatasourcesRowset(request, handler);
      }
   },

   /**
    * 
    * 
    * 
    * restrictions
    * 
    * Not supported
    */
   DISCOVER_PROPERTIES(1, "Returns a list of information and values about the requested " + "properties that are supported by the specified data source " + "provider.",
            new Column[] { DiscoverPropertiesRowset.PropertyName, DiscoverPropertiesRowset.PropertyDescription, DiscoverPropertiesRowset.PropertyType,
                     DiscoverPropertiesRowset.PropertyAccessType, DiscoverPropertiesRowset.IsRequired, DiscoverPropertiesRowset.Value, }, null /*
                                                                                                                                                * not
                                                                                                                                                * sorted
                                                                                                                                                */) {
      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DiscoverPropertiesRowset(request, handler);
      }
   },

   /**
    * Note that SQL Server also returns the data-mining columns.
    * 
    * 
    * restrictions
    * 
    * Not supported
    */
   DISCOVER_SCHEMA_ROWSETS(2, "Returns the names, values, and other information of all supported " + "RequestType enumeration values.", new Column[] {
            DiscoverSchemaRowsetsRowset.SchemaName, DiscoverSchemaRowsetsRowset.SchemaGuid, DiscoverSchemaRowsetsRowset.Restrictions, DiscoverSchemaRowsetsRowset.Description,
            DiscoverSchemaRowsetsRowset.RestrictionsMask, }, null /* not sorted */) {
      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DiscoverSchemaRowsetsRowset(request, handler);
      }

      protected void writeRowsetXmlSchemaRowDef(SaxWriter writer) {
         writer.startElement("xsd:complexType", "name", "row");
         writer.startElement("xsd:sequence");
         for (Column column : columnDefinitions) {
            final String name = XmlaUtil.ElementNameEncoder.INSTANCE.encode(column.name);

            if (column == DiscoverSchemaRowsetsRowset.Restrictions) {
               writer.startElement("xsd:element", "sql:field", column.name, "name", name, "minOccurs", 0, "maxOccurs", "unbounded");
               writer.startElement("xsd:complexType");
               writer.startElement("xsd:sequence");
               writer.element("xsd:element", "name", "Name", "type", "xsd:string", "sql:field", "Name");
               writer.element("xsd:element", "name", "Type", "type", "xsd:string", "sql:field", "Type");

               writer.endElement(); // xsd:sequence
               writer.endElement(); // xsd:complexType
               writer.endElement(); // xsd:element

            } else {
               final String xsdType = column.type.columnType;

               Object[] attrs;
               if (column.nullable) {
                  if (column.unbounded) {
                     attrs = new Object[] { "sql:field", column.name, "name", name, "type", xsdType, "minOccurs", 0, "maxOccurs", "unbounded" };
                  } else {
                     attrs = new Object[] { "sql:field", column.name, "name", name, "type", xsdType, "minOccurs", 0 };
                  }
               } else {
                  if (column.unbounded) {
                     attrs = new Object[] { "sql:field", column.name, "name", name, "type", xsdType, "maxOccurs", "unbounded" };
                  } else {
                     attrs = new Object[] { "sql:field", column.name, "name", name, "type", xsdType };
                  }
               }
               writer.element("xsd:element", attrs);
            }
         }
         writer.endElement(); // xsd:sequence
         writer.endElement(); // xsd:complexType
      }
   },

   DISCOVER_ENUMERATORS(20, null, new Column[] { DiscoverEnumeratorsSchema.EnumName }, new Column[] { DiscoverEnumeratorsSchema.EnumName }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DiscoverEnumeratorsSchema(request, handler);
      }
   },

   DISCOVER_KEYWORDS(20, null, new Column[] { DiscoverKeyWordsSchema.Keyword }, new Column[] { DiscoverKeyWordsSchema.Keyword }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new MdschemaMeasureGroupRowset(request, handler);
      }
   },

   DISCOVER_LITERALS(5, "Returns information about literals supported by the provider.", new Column[] { DiscoverLiteralsRowset.LiteralName, DiscoverLiteralsRowset.LiteralValue,
            DiscoverLiteralsRowset.LiteralInvalidChars, DiscoverLiteralsRowset.LiteralInvalidStartingChars, DiscoverLiteralsRowset.LiteralMaxLength,
            DiscoverLiteralsRowset.LiteralNameEnumValue, // used by SSAS 12
   }, null /* not sorted */) {
      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DiscoverLiteralsRowset(request, handler);
      }
   },

   DISCOVER_XML_METADATA(20, null, new Column[] { DiscoverXmlMetaData.DatabaseID, DiscoverXmlMetaData.DimensionPermissionID, DiscoverXmlMetaData.CubeID,
            DiscoverXmlMetaData.MeasureGroupID, DiscoverXmlMetaData.PartitionID, DiscoverXmlMetaData.PerspectiveID, DiscoverXmlMetaData.DimensionPermissionID,
            DiscoverXmlMetaData.RoleID, DiscoverXmlMetaData.DatabasePermissionID, DiscoverXmlMetaData.MiningModelID, DiscoverXmlMetaData.MiningModelPermissionID,
            DiscoverXmlMetaData.DataSourceID, DiscoverXmlMetaData.MiningStructureID, DiscoverXmlMetaData.AggregationDesignID, DiscoverXmlMetaData.TraceID,
            DiscoverXmlMetaData.MiningStructurePermissionID, DiscoverXmlMetaData.CubePermissionID, DiscoverXmlMetaData.AssemblyID, DiscoverXmlMetaData.MdxScriptID,
            DiscoverXmlMetaData.DataSourceViewID, DiscoverXmlMetaData.DataSourcePermissionID, DiscoverXmlMetaData.CalculatedColumns, DiscoverXmlMetaData.ObjectExpansion, },

   new Column[] { DiscoverXmlMetaData.DatabaseID, DiscoverXmlMetaData.DimensionPermissionID, DiscoverXmlMetaData.CubeID, DiscoverXmlMetaData.MeasureGroupID,
            DiscoverXmlMetaData.PartitionID, DiscoverXmlMetaData.PerspectiveID, DiscoverXmlMetaData.DimensionPermissionID, DiscoverXmlMetaData.RoleID,
            DiscoverXmlMetaData.DatabasePermissionID, DiscoverXmlMetaData.MiningModelID, DiscoverXmlMetaData.MiningModelPermissionID, DiscoverXmlMetaData.DataSourceID,
            DiscoverXmlMetaData.MiningStructureID, DiscoverXmlMetaData.AggregationDesignID, DiscoverXmlMetaData.TraceID, DiscoverXmlMetaData.MiningStructurePermissionID,
            DiscoverXmlMetaData.CubePermissionID, DiscoverXmlMetaData.AssemblyID, DiscoverXmlMetaData.MdxScriptID, DiscoverXmlMetaData.DataSourceViewID,
            DiscoverXmlMetaData.DataSourcePermissionID, DiscoverXmlMetaData.CalculatedColumns, DiscoverXmlMetaData.ObjectExpansion, }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DiscoverXmlMetaData(request, handler);
      }
   },

   DISCOVER_TRACES(15, "", new Column[] { DiscoverTracesRowset.TraceId, DiscoverTracesRowset.Type }, null) {
      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DiscoverTracesRowset(request, handler);
      }

   },
   DISCOVER_TRACE_DEFINITION_PROVIDERINFO(15, "", new Column[] { DiscoverTraceDefinitionProviderInfo.Data }, null) {
      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DiscoverTraceDefinitionProviderInfo(request, handler);
      }
   },

   DISCOVER_XEVENT_TRACE_DEFINITION(15, "", new Column[] { DiscoverXeventTraceDefinition.Data }, null) {
      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DiscoverXeventTraceDefinition(request, handler);
      }
   },

   DISCOVER_TRACE_COLUMNS(15, "", new Column[] { DiscoverTraceColumns.Data }, null) {
      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DiscoverTraceColumns(request, handler);
      }
   },

   DISCOVER_TRACE_EVENT_CATEGORIES(15, "", new Column[] { DiscoverTraceEventCategories.Data }, null) {
      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DiscoverTraceEventCategories(request, handler);
      }
   },

   DISCOVER_MEMORYUSAGE(15, "", new Column[] { DiscoverMemoryUsage.SPID, DiscoverMemoryUsage.MemoryUsed, DiscoverMemoryUsage.BaseObjectType, DiscoverMemoryUsage.Shrinkable }, null /*
                                                                                                                                                                                     * not
                                                                                                                                                                                     * sorted
                                                                                                                                                                                     */) {
      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DiscoverMemoryUsage(request, handler);
      }
   },

   DISCOVER_MEMORYGRANT(15, "", new Column[] { DiscoveryMemoryGrant.SPID }, null) {
      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DiscoveryMemoryGrant(request, handler);
      }
   },

   DISCOVER_LOCKS(20, null, new Column[] { DiscoverLocksRowset.SPID, DiscoverLocksRowset.LockTransactionId, DiscoverLocksRowset.LockObjectId, DiscoverLocksRowset.LockStauts,

   DiscoverLocksRowset.LockType, DiscoverLocksRowset.LockMinTotalMs, },

   new Column[] { DiscoverLocksRowset.SPID, DiscoverLocksRowset.LockTransactionId, DiscoverLocksRowset.LockObjectId, DiscoverLocksRowset.LockStauts,

   DiscoverLocksRowset.LockType, DiscoverLocksRowset.LockMinTotalMs, }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DiscoverLocksRowset(request, handler);
      }
   },

   DISCOVER_CONNECTIONS(20, null, new Column[] { DiscoverConnectionsRowset.ConnectionId, DiscoverConnectionsRowset.ConnectionUserName,
            DiscoverConnectionsRowset.ConnectionImpersonatedUserName, DiscoverConnectionsRowset.ConnectionHostName, DiscoverConnectionsRowset.ConnectionElapsedTiemMs,
            DiscoverConnectionsRowset.ConnectionLastCommandElapsedTimeMs, DiscoverConnectionsRowset.ConnectionIdleTimeMs, },

   new Column[] { DiscoverConnectionsRowset.ConnectionId, DiscoverConnectionsRowset.ConnectionUserName, DiscoverConnectionsRowset.ConnectionImpersonatedUserName,
            DiscoverConnectionsRowset.ConnectionHostName, DiscoverConnectionsRowset.ConnectionElapsedTiemMs, DiscoverConnectionsRowset.ConnectionLastCommandElapsedTimeMs,
            DiscoverConnectionsRowset.ConnectionIdleTimeMs, }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DiscoverConnectionsRowset(request, handler);
      }
   },

   DISCOVER_SESSIONS(20, null, new Column[] { DiscoverSessionsRowset.SessionId, DiscoverSessionsRowset.SessionSpId, DiscoverSessionsRowset.SessionConnectionId,
            DiscoverSessionsRowset.SessionUserName, DiscoverSessionsRowset.SessionCurrentDatabase, DiscoverSessionsRowset.SessionElapsedTimeMs,
            DiscoverSessionsRowset.SessionCpuTimeMs, DiscoverSessionsRowset.SessionIdleTimeMs, DiscoverSessionsRowset.SessionStatus, },

   new Column[] { DiscoverSessionsRowset.SessionId, DiscoverSessionsRowset.SessionSpId, DiscoverSessionsRowset.SessionConnectionId, DiscoverSessionsRowset.SessionUserName,
            DiscoverSessionsRowset.SessionCurrentDatabase, DiscoverSessionsRowset.SessionElapsedTimeMs, DiscoverSessionsRowset.SessionCpuTimeMs,
            DiscoverSessionsRowset.SessionIdleTimeMs, DiscoverSessionsRowset.SessionStatus, }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DiscoverSessionsRowset(request, handler);
      }
   },

   DISCOVER_JOBS(20, null, new Column[] { DiscoverJobsRowset.SpId, DiscoverJobsRowset.JobId, DiscoverJobsRowset.JobDescription, DiscoverJobsRowset.JobThreadPollId,
            DiscoverJobsRowset.JobMinTotalTimeMs, },

   new Column[] { DiscoverJobsRowset.SpId, DiscoverJobsRowset.JobId, DiscoverJobsRowset.JobDescription, DiscoverJobsRowset.JobThreadPollId, DiscoverJobsRowset.JobMinTotalTimeMs, }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DiscoverJobsRowset(request, handler);
      }
   },

   DISCOVER_TRANSACTIONS(20, null, new Column[] { DiscoverTransactionsRowset.TransactionId, DiscoverTransactionsRowset.TransactionSessionId }, new Column[] {
            DiscoverTransactionsRowset.TransactionId, DiscoverTransactionsRowset.TransactionSessionId }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DiscoverTransactionsRowset(request, handler);
      }
   },

   DISCOVER_DB_CONNECTIONS(0, "Returns a list of XML for Analysis data sources available on the " + "", new Column[] { DiscoverDBConnectionsRowset.ConnectionId,
            DiscoverDBConnectionsRowset.ConnectionInUser, DiscoverDBConnectionsRowset.ConnectionServerName, DiscoverDBConnectionsRowset.ConnectionCatalogName,
            DiscoverDBConnectionsRowset.ConnectionSpId },
   // XMLA does not specify a sort order, but olap4j does.
            new Column[] { DiscoverDBConnectionsRowset.ConnectionId, DiscoverDBConnectionsRowset.ConnectionInUser, DiscoverDBConnectionsRowset.ConnectionServerName,
                     DiscoverDBConnectionsRowset.ConnectionCatalogName, DiscoverDBConnectionsRowset.ConnectionSpId }) {
      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DiscoverDatasourcesRowset(request, handler);
      }
   },

   DISCOVER_MASTER_KEY(20, null, new Column[] { DiscoverMasterKey.Key }, new Column[] { DiscoverMasterKey.Key }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DiscoverMasterKey(request, handler);
      }
   },

   DISCOVER_PERFORMANCE_COUNTERS(20, null, new Column[] { DiscoverPerformanceCounterSchema.PrefCounterName }, new Column[] { DiscoverPerformanceCounterSchema.PrefCounterName }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DiscoverPerformanceCounterSchema(request, handler);
      }
   },

   DISCOVER_LOCATIONS(20, null, new Column[] { DiscoverLocationsRowset.LocationBackUpFilePathName,  DiscoverLocationsRowset.LocationPassWord },

   new Column[] {DiscoverLocationsRowset.LocationBackUpFilePathName, DiscoverLocationsRowset.LocationPassWord  }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DiscoverLocationsRowset(request, handler);
      }
   },

   DISCOVER_PARTITION_DIMENSION_STAT(20, null, new Column[] { DiscoverPartitionDimensionStatSchema.DatabaseName, DiscoverPartitionDimensionStatSchema.CubeName,
            DiscoverPartitionDimensionStatSchema.MeasureGroupName, DiscoverPartitionDimensionStatSchema.PartitionName }, new Column[] {
            DiscoverPartitionDimensionStatSchema.DatabaseName, DiscoverPartitionDimensionStatSchema.CubeName, DiscoverPartitionDimensionStatSchema.MeasureGroupName,
            DiscoverPartitionDimensionStatSchema.PartitionName }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DiscoverPerformanceCounterSchema(request, handler);
      }
   },

   // DISCOVER_PARTITION_STAT
   DISCOVER_PARTITION_STAT(20, null, new Column[] { DiscoverPartitionStateSchema.DatabaseName, DiscoverPartitionStateSchema.CubeName,
            DiscoverPartitionStateSchema.MeasureGroupName, DiscoverPartitionStateSchema.PartitionName }, new Column[] { DiscoverPartitionStateSchema.DatabaseName,
            DiscoverPartitionStateSchema.CubeName, DiscoverPartitionStateSchema.MeasureGroupName, DiscoverPartitionStateSchema.PartitionName }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DiscoverPerformanceCounterSchema(request, handler);
      }
   },

   DISCOVER_DIMENSION_STAT(20, null, new Column[] { DiscoverDimensionStatRowset.DatabaseName,DiscoverDimensionStatRowset.DimensionName },

   new Column[] { DiscoverDimensionStatRowset.DatabaseName,DiscoverDimensionStatRowset.DimensionName }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DiscoverLocationsRowset(request, handler);
      }
   },

   DISCOVER_COMMANDS(20, null, new Column[] { DiscoverCommandsRowset.SessionSpId },

   new Column[] { DiscoverCommandsRowset.SessionSpId }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DiscoverCommandsRowset(request, handler);
      }
   },

   DISCOVER_COMMAND_OBJECTS(20, null, new Column[] { DiscoverCommandObjectsRowset.SessionSpId, DiscoverCommandObjectsRowset.SessionId,
            DiscoverCommandObjectsRowset.ObjectParentPath, DiscoverCommandObjectsRowset.ObjectId },

   new Column[] { DiscoverCommandObjectsRowset.SessionSpId, DiscoverCommandObjectsRowset.SessionId, DiscoverCommandObjectsRowset.ObjectParentPath,
            DiscoverCommandObjectsRowset.ObjectId }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DiscoverCommandObjectsRowset(request, handler);
      }
   },

   DISCOVER_OBJECT_ACTIVITY(20, null, new Column[] { DiscoverObjectActivityRowset.ObjectParentPath, DiscoverObjectActivityRowset.ObjectId },

   new Column[] { DiscoverObjectActivityRowset.ObjectParentPath, DiscoverObjectActivityRowset.ObjectId }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DiscoverObjectActivityRowset(request, handler);
      }
   },

   DISCOVER_OBJECT_MEMORY_USAGE(20, null, new Column[] { DiscoverObjectMemoryUsageRowset.ObjectParentPath, DiscoverObjectMemoryUsageRowset.ObjectId },

   new Column[] { DiscoverObjectMemoryUsageRowset.ObjectParentPath, DiscoverObjectMemoryUsageRowset.ObjectId }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DiscoverObjectMemoryUsageRowset(request, handler);
      }
   },

   DISCOVER_STORAGE_TABLES(20, null, new Column[] { DiscoverStorageTablesRowset.DatabaseName, DiscoverStorageTablesRowset.CubeName, DiscoverStorageTablesRowset.MeasureGroupName },

   new Column[] { DiscoverStorageTablesRowset.DatabaseName, DiscoverStorageTablesRowset.CubeName, DiscoverStorageTablesRowset.MeasureGroupName }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DiscoverStorageTablesRowset(request, handler);
      }
   },

   DISCOVER_STORAGE_TABLE_COLUMNS(20, null, new Column[] { DiscoverStorageTableColumnsRowset.DatabaseName, DiscoverStorageTableColumnsRowset.CubeName,
            DiscoverStorageTableColumnsRowset.MeasureGroupName },

   new Column[] { DiscoverStorageTableColumnsRowset.DatabaseName, DiscoverStorageTableColumnsRowset.CubeName, DiscoverStorageTableColumnsRowset.MeasureGroupName }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DiscoverStorageTableColumnsRowset(request, handler);
      }
   },

   DISCOVER_STORAGE_TABLE_COLUMN_SEGMENTS(20, null, new Column[] { DiscoverStorageTableColumnSegments.DatabaseName, DiscoverStorageTableColumnSegments.CubeName,
            DiscoverStorageTableColumnSegments.MeasureGroupName, DiscoverStorageTableColumnSegments.PartitionName },

   new Column[] { DiscoverStorageTableColumnSegments.DatabaseName, DiscoverStorageTableColumnSegments.CubeName, DiscoverStorageTableColumnSegments.MeasureGroupName,
            DiscoverStorageTableColumnSegments.PartitionName }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DiscoverStorageTableColumnSegments(request, handler);
      }
   },

   DISCOVER_CALC_DEPENDENCY(20, null, new Column[] { DiscoverCalcDependencyRowset.DatabaseName, DiscoverCalcDependencyRowset.ObjectType, DiscoverCalcDependencyRowset.Query },

   new Column[] { DiscoverCalcDependencyRowset.DatabaseName, DiscoverCalcDependencyRowset.ObjectType, DiscoverCalcDependencyRowset.Query }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DiscoverCalcDependencyRowset(request, handler);
      }
   },

   DISCOVER_CSDL_METADATA(20, null, new Column[] { DiscoverCsdlMetaData.CatalogName, DiscoverCsdlMetaData.PerspectiveName, DiscoverCsdlMetaData.Version },

   new Column[] { DiscoverCsdlMetaData.CatalogName, DiscoverCsdlMetaData.PerspectiveName, DiscoverCsdlMetaData.Version }) {

      public Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler) {
         return new DiscoverCsdlMetaData(request, handler);
      }
   };

   // //End

   /**
    * 
    * 
    * 
    * restrictions
    * 
    * Not supported
    */

   // DBSCHEMA_SCHEMATA(
   // 8, null,
   // new Column[] {
   // DbschemaSchemataRowset.CatalogName,
   // DbschemaSchemataRowset.SchemaName,
   // DbschemaSchemataRowset.SchemaOwner,
   // },
   // new Column[] {
   // DbschemaSchemataRowset.CatalogName,
   // DbschemaSchemataRowset.SchemaName,
   // DbschemaSchemataRowset.SchemaOwner,
   // })
   // {
   // public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
   // return new DbschemaSchemataRowset(request, handler);
   // }
   // },

   /**
    * http://msdn.microsoft.com/library/en-us/oledb/htm/
    * oledbtables_info_rowset.asp
    * 
    * 
    * restrictions
    * 
    * Not supported
    */
   // DBSCHEMA_TABLES_INFO(
   // 10, null,
   // new Column[] {
   // DbschemaTablesInfoRowset.TableCatalog,
   // DbschemaTablesInfoRowset.TableSchema,
   // DbschemaTablesInfoRowset.TableName,
   // DbschemaTablesInfoRowset.TableType,
   // DbschemaTablesInfoRowset.TableGuid,
   // DbschemaTablesInfoRowset.Bookmarks,
   // DbschemaTablesInfoRowset.BookmarkType,
   // DbschemaTablesInfoRowset.BookmarkDataType,
   // DbschemaTablesInfoRowset.BookmarkMaximumLength,
   // DbschemaTablesInfoRowset.BookmarkInformation,
   // DbschemaTablesInfoRowset.TableVersion,
   // DbschemaTablesInfoRowset.Cardinality,
   // DbschemaTablesInfoRowset.Description,
   // DbschemaTablesInfoRowset.TablePropId,
   // },
   // null /* cannot find doc -- presume unsorted */)
   // {
   // public Rowset getRowset(XmlaRequest request, XmlaHandler handler) {
   // return new DbschemaTablesInfoRowset(request, handler);
   // }
   // },

   transient final Column[] columnDefinitions;
   transient final Column[] sortColumnDefinitions;

   /**
    * Date the schema was last modified.
    * 
    * <p>
    * TODO: currently schema grammar does not support modify date so we return
    * just some date for now.
    */
   private static final String dateModified = "2005-01-25T17:35:32";
   private final String description;

   static final String UUID_PATTERN = "[0-9a-zA-Z]{8}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{12}";

   /**
    * Creates a rowset definition.
    * 
    * @param ordinal
    *           Rowset ordinal, per OLE DB for OLAP
    * @param description
    *           Description
    * @param columnDefinitions
    *           List of column definitions
    * @param sortColumnDefinitions
    *           List of column definitions to sort on,
    */
   RowsetDefinition(int ordinal, String description, Column[] columnDefinitions, Column[] sortColumnDefinitions) {
      Util.discard(ordinal);
      this.description = description;
      this.columnDefinitions = columnDefinitions;
      this.sortColumnDefinitions = sortColumnDefinitions;
   }

   public abstract Rowset getRowset(XmlaRequest request, CustomXmlaHandler handler);

   public Column lookupColumn(String name) {
      for (Column columnDefinition : columnDefinitions) {
         if (columnDefinition.name.equals(name)) {
            return columnDefinition;
         }
      }
      return null;
   }

   /**
    * Returns a comparator with which to sort rows of this rowset definition.
    * The sort order is defined by the {@link #sortColumnDefinitions} field. If
    * the rowset is not sorted, returns null.
    */
   Comparator<Rowset.Row> getComparator() {
      if (sortColumnDefinitions == null) {
         return null;
      }
      return new Comparator<Rowset.Row>() {
         public int compare(Rowset.Row row1, Rowset.Row row2) {
            // A faster implementation is welcome.
            for (Column sortColumn : sortColumnDefinitions) {
               Comparable val1 = (Comparable) row1.get(sortColumn.name);
               Comparable val2 = (Comparable) row2.get(sortColumn.name);
               if ((val1 == null) && (val2 == null)) {
                  // columns can be optional, compare next column
                  continue;
               } else if (val1 == null) {
                  return -1;
               } else if (val2 == null) {
                  return 1;
               } else if (val1 instanceof String && val2 instanceof String) {
                  int v = ((String) val1).compareToIgnoreCase((String) val2);
                  // if equal (= 0), compare next column
                  if (v != 0) {
                     return v;
                  }
               } else {
                  int v = val1.compareTo(val2);
                  // if equal (= 0), compare next column
                  if (v != 0) {
                     return v;
                  }
               }
            }
            return 0;
         }
      };
   }

   /**
    * Generates an XML schema description to the writer. This is broken into
    * top, Row definition and bottom so that on a case by case basis a
    * RowsetDefinition can redefine the Row definition output. The default
    * assumes a flat set of elements, but for example, SchemaRowsets has a
    * element with child elements.
    * 
    * @param writer
    *           SAX writer
    * @see CustomXmlaHandler#writeDatasetXmlSchema(SaxWriter,
    *      mondrian.xmla.XmlaHandler.SetType)
    */
   void writeRowsetXmlSchema(SaxWriter writer) {
      writeRowsetXmlSchemaTop(writer);
      writeRowsetXmlSchemaRowDef(writer);
      writeRowsetXmlSchemaBottom(writer);
   }

   protected void writeRowsetXmlSchemaTop(SaxWriter writer) {
      writer.startElement("xsd:schema", "xmlns:sql", "urn:schemas-microsoft-com:xml-sql", "targetNamespace", NS_XMLA_ROWSET, "elementFormDefault", "qualified");

      writer.startElement("xsd:element", "name", "root");
      writer.startElement("xsd:complexType");
      writer.startElement("xsd:sequence", "minOccurs", 0, "maxOccurs", "unbounded");
      writer.element("xsd:element", "name", "row", "type", "row");
      writer.endElement(); // xsd:sequence
      writer.endElement(); // xsd:complexType
      writer.endElement(); // xsd:element

      // MS SQL includes this in its schema section even thought
      // its not need for most queries.
      writer.startElement("xsd:simpleType", "name", "uuid");
      writer.startElement("xsd:restriction", "base", "xsd:string");
      writer.element("xsd:pattern", "value", UUID_PATTERN);

      writer.endElement(); // xsd:restriction
      writer.endElement(); // xsd:simpleType
   }

   protected void writeRowsetXmlSchemaRowDef(SaxWriter writer) {

      Map<String, Object[]> tmpMap = new HashMap<String, Object[]>();
      List<Object[]> tmpList = new ArrayList<Object[]>();
      for (Column column : columnDefinitions) {
         final String name = XmlaUtil.ElementNameEncoder.INSTANCE.encode(column.name);
         final String xsdType = column.type.columnType;

         Object[] attrs;
         if (column.nullable) {
            if (column.unbounded) {
               attrs = new Object[] { "sql:field", column.name, "name", name, "type", xsdType, "minOccurs", 0, "maxOccurs", "unbounded" };
            } else {
               attrs = new Object[] { "sql:field", column.name, "name", name, "type", xsdType, "minOccurs", 0 };
            }
         } else {
            if (column.unbounded) {
               attrs = new Object[] { "sql:field", column.name, "name", name, "type", xsdType, "maxOccurs", "unbounded" };
            } else {
               attrs = new Object[] { "sql:field", column.name, "name", name, "type", xsdType };
            }
         }

         tmpMap.put(column.name, attrs);
         tmpList.add(attrs);
      }

      // This's the right order of elements under complex type
      final String[] nameList = { "CATALOG_NAME", "SCHEMA_NAME", "CUBE_NAME", "DIMENSION_UNIQUE_NAME", "HIERARCHY_UNIQUE_NAME", "LEVEL_UNIQUE_NAME", "MEMBER_UNIQUE_NAME",
               "PROPERTY_TYPE", "PROPERTY_NAME", "PROPERTY_CAPTION", "DATA_TYPE", "CHARACTER_MAXIMUM_LENGTH", "CHARACTER_OCTET_LENGTH", "NUMERIC_PRECISION", "NUMERIC_SCALE",
               "DESCRIPTION", "PROPERTY_CONTENT_TYPE", "SQL_COLUMN_NAME", "LANGUAGE", "PROPERTY_ORIGIN", "PROPERTY_ATTRIBUTE_HIERARCHY_NAME", "PROPERTY_CARDINALITY", "MIME_TYPE",
               "PROPERTY_IS_VISIBLE" };

      boolean isMdschemaProperties = false;

      for (String s : nameList) {
         isMdschemaProperties = true;
         if (!tmpMap.containsKey(s)) {
            isMdschemaProperties = false;
            break;
         }
      }
      writer.startElement("xsd:complexType", "name", "row");
      writer.startElement("xsd:sequence");
      if (isMdschemaProperties) {
         for (String s : nameList) {
            writer.element("xsd:element", (Object[]) (tmpMap.get(s)));
         }
      } else {
         for (Object o : tmpList) {
            writer.element("xsd:element", (Object[]) o);
         }
      }
      writer.endElement(); // xsd:sequence
      writer.endElement(); // xsd:complexType
   }

   protected void writeRowsetXmlSchemaBottom(SaxWriter writer) {
      writer.endElement(); // xsd:schema
   }

   enum Type {
      String("xsd:string"), StringArray("xsd:string"), Array("xsd:string"), Enumeration("xsd:string"), EnumerationArray("xsd:string"), EnumString("xsd:string"), Boolean(
               "xsd:boolean"), StringSometimesArray("xsd:string"), Integer("xsd:int"), UnsignedInteger("xsd:unsignedInt"), DateTime("xsd:dateTime"), Rowset(null), Short(
               "xsd:short"), UUID("uuid"), UnsignedShort("xsd:unsignedShort"), Long("xsd:long"), UnsignedLong("xsd:unsignedLong");

      public final String columnType;

      Type(String columnType) {
         this.columnType = columnType;
      }

      boolean isEnum() {
         return this == Enumeration || this == EnumerationArray || this == EnumString;
      }

      String getName() {
         return this == String ? "string" : name();
      }
   }

   private static XmlaConstants.DBType getDBTypeFromProperty(Property prop) {
      switch (prop.getDatatype()) {
      case STRING:
         return XmlaConstants.DBType.WSTR;
      case INTEGER:
      case UNSIGNED_INTEGER:
      case DOUBLE:
         return XmlaConstants.DBType.R8;
      case BOOLEAN:
         return XmlaConstants.DBType.BOOL;
      default:
         return XmlaConstants.DBType.WSTR;
      }
   }

   static class Column {

      /**
       * This is used as the true value for the restriction parameter.
       */
      static final boolean RESTRICTION = true;
      /**
       * This is used as the false value for the restriction parameter.
       */
      static final boolean NOT_RESTRICTION = false;

      /**
       * This is used as the false value for the nullable parameter.
       */
      static final boolean REQUIRED = false;
      /**
       * This is used as the true value for the nullable parameter.
       */
      static final boolean OPTIONAL = true;

      /**
       * This is used as the false value for the unbounded parameter.
       */
      static final boolean ONE_MAX = false;
      /**
       * This is used as the true value for the unbounded parameter.
       */
      static final boolean UNBOUNDED = true;

      final String name;
      final Type type;
      final Enumeration enumeration;
      final String description;
      final boolean restriction;
      final boolean nullable;
      final boolean unbounded;

      /**
       * Creates a column.
       * 
       * @param name
       *           Name of column
       * @param type
       *           A {@link mondrian.xmla.RowsetDefinition.Type} value
       * @param enumeratedType
       *           Must be specified for enumeration or array of enumerations
       * @param description
       *           Description of column
       * @param restriction
       *           Whether column can be used as a filter on its rowset
       * @param nullable
       *           Whether column can contain null values
       * @pre type != null
       * @pre (type == Type.Enumeration || type == Type.EnumerationArray || type
       *      == Type.EnumString) == (enumeratedType != null)
       * @pre description == null || description.indexOf('\r') == -1
       */
      Column(String name, Type type, Enumeration enumeratedType, boolean restriction, boolean nullable, String description) {
         this(name, type, enumeratedType, restriction, nullable, ONE_MAX, description);
      }

      Column(String name, Type type, Enumeration enumeratedType, boolean restriction, boolean nullable, boolean unbounded, String description) {
         assert type != null;
         assert (type == Type.Enumeration || type == Type.EnumerationArray || type == Type.EnumString) == (enumeratedType != null);
         // Line endings must be UNIX style (LF) not Windows style (LF+CR).
         // Thus the client will receive the same XML, regardless
         // of the server O/S.
         assert description == null || description.indexOf('\r') == -1;
         this.name = name;
         this.type = type;
         this.enumeration = enumeratedType;
         this.description = description;
         this.restriction = restriction;
         this.nullable = nullable;
         this.unbounded = unbounded;
      }

      /**
       * Retrieves a value of this column from a row. The base implementation
       * uses reflection to call an accessor method; a derived class may provide
       * a different implementation.
       * 
       * @param row
       *           Row
       */
      protected Object get(Object row) {
         return getFromAccessor(row);
      }

      /**
       * Retrieves the value of this column "MyColumn" from a field called
       * "myColumn".
       * 
       * @param row
       *           Current row
       * @return Value of given this property of the given row
       */
      protected final Object getFromField(Object row) {
         try {
            String javaFieldName = name.substring(0, 1).toLowerCase() + name.substring(1);
            Field field = row.getClass().getField(javaFieldName);
            return field.get(row);
         } catch (NoSuchFieldException e) {
            throw Util.newInternal(e, "Error while accessing rowset column " + name);
         } catch (SecurityException e) {
            throw Util.newInternal(e, "Error while accessing rowset column " + name);
         } catch (IllegalAccessException e) {
            throw Util.newInternal(e, "Error while accessing rowset column " + name);
         }
      }

      /**
       * Retrieves the value of this column "MyColumn" by calling a method
       * called "getMyColumn()".
       * 
       * @param row
       *           Current row
       * @return Value of given this property of the given row
       */
      protected final Object getFromAccessor(Object row) {
         try {
            String javaMethodName = "get" + name;
            Method method = row.getClass().getMethod(javaMethodName);
            return method.invoke(row);
         } catch (SecurityException e) {
            throw Util.newInternal(e, "Error while accessing rowset column " + name);
         } catch (IllegalAccessException e) {
            throw Util.newInternal(e, "Error while accessing rowset column " + name);
         } catch (NoSuchMethodException e) {
            throw Util.newInternal(e, "Error while accessing rowset column " + name);
         } catch (InvocationTargetException e) {
            throw Util.newInternal(e, "Error while accessing rowset column " + name);
         }
      }

      public String getColumnType() {
         if (type.isEnum()) {
            return enumeration.type.columnType;
         }
         return type.columnType;
      }
   }

   // -------------------------------------------------------------------------
   // From this point on, just rowset classess.

   static class DiscoverSchemaRowsetsRowset extends Rowset {
      private static final Column SchemaName = new Column("SchemaName", Type.StringArray, null, Column.RESTRICTION, Column.REQUIRED,
               "The name of the schema/request. This returns the values in " + "the RequestTypes enumeration, plus any additional types "
                        + "supported by the provider. The provider defines rowset " + "structures for the additional types");
      private static final Column SchemaGuid = new Column("SchemaGuid", Type.UUID, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "The GUID of the schema.");
      private static final Column Restrictions = new Column("Restrictions", Type.Array, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "An array of the restrictions suppoted by provider. An example " + "follows this table.");
      private static final Column Description = new Column("Description", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL,

      "A localizable description of the schema");

      private static final Column RestrictionsMask = new Column("RestrictionsMask", Type.UnsignedLong, null, Column.NOT_RESTRICTION,
      // Column.REQUIRED,
               Column.OPTIONAL,

               "The lowest N bits set to 1, where N is the number of restrictions.");

      private List restrictedSchemas = null;

      public DiscoverSchemaRowsetsRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(DISCOVER_SCHEMA_ROWSETS, request, handler);

         if (request.getRestrictions() != null && request.getRestrictions().size() > 0) {
            Map restrictions = request.getRestrictions();
            restrictedSchemas = (List<String>) restrictions.get("SchemaName");
         }
      }

      public void populateImpl(XmlaResponse response, OlapConnection connection, List<Row> rows) throws XmlaException {
         RowsetDefinition[] rowsetDefinitions = RowsetDefinition.class.getEnumConstants().clone();
         // Arrays.sort(rowsetDefinitions, new Comparator<RowsetDefinition>() {
         // public int compare(RowsetDefinition o1, RowsetDefinition o2) {
         // return o1.name().compareTo(o2.name());
         // }
         // });
         for (RowsetDefinition rowsetDefinition : rowsetDefinitions) {
            if (restrictedSchemas == null || (restrictedSchemas != null && restrictedSchemas.contains(rowsetDefinition.name()))) {
               Row row = new Row();

               row.set(SchemaName.name, rowsetDefinition.name());
               row.set(SchemaGuid.name, UUID.fromString(getSchemGuid(rowsetDefinition)));
               row.set(Restrictions.name, getRestrictions(rowsetDefinition));
               row.set(RestrictionsMask.name, getRestrictionsMask(rowsetDefinition));
               addRow(row, rows);
            }
         }
      }

      private List<XmlElement> getRestrictions(RowsetDefinition rowsetDefinition) {

         List<XmlElement> restrictionList = new ArrayList<XmlElement>();
         final Column[] columns = rowsetDefinition.columnDefinitions;
         for (Column column : columns) {
            if (column.restriction) {
               restrictionList.add(new XmlElement(Restrictions.name, null, new XmlElement[] { new XmlElement("Name", null, column.name),
                        new XmlElement("Type", null, column.getColumnType()) }));

            }
         }
         return restrictionList;
      }

      private String getRestrictionsMask(RowsetDefinition rowsetDefinition) {

         // Ignore
         if (rowsetDefinition.equals(DBSCHEMA_CATALOGS))
            return "1";
         if (rowsetDefinition.equals(DBSCHEMA_TABLES))
            return "31";
         if (rowsetDefinition.equals(DBSCHEMA_COLUMNS))
            return "31";
         if (rowsetDefinition.equals(DBSCHEMA_PROVIDER_TYPES))
            return "3";
         if (rowsetDefinition.equals(DISCOVER_DATASOURCES))
            return "31";

         if (rowsetDefinition.equals(DISCOVER_LITERALS))
            return "1";
         if (rowsetDefinition.equals(DISCOVER_PROPERTIES))
            return "1";
         if (rowsetDefinition.equals(DISCOVER_SCHEMA_ROWSETS))
            return "1";
         if (rowsetDefinition.equals(MDSCHEMA_ACTIONS))
            return "511";
         if (rowsetDefinition.equals(MDSCHEMA_CUBES))
            return "31";
         if (rowsetDefinition.equals(MDSCHEMA_DIMENSIONS))
            return "127";
         if (rowsetDefinition.equals(MDSCHEMA_FUNCTIONS))
            return "15";
         if (rowsetDefinition.equals(MDSCHEMA_HIERARCHIES))
            return "511";
         if (rowsetDefinition.equals(MDSCHEMA_LEVELS))
            return "1023";
         if (rowsetDefinition.equals(MDSCHEMA_MEASURES))
            return "255";
         if (rowsetDefinition.equals(MDSCHEMA_MEMBERS))
            return "16383";
         if (rowsetDefinition.equals(MDSCHEMA_PROPERTIES))
            return "8191";
         if (rowsetDefinition.equals(MDSCHEMA_SETS))
            return "255";
         if (rowsetDefinition.equals(MDSCHEMA_MEASUREGROUPS))
            return "15";
         if (rowsetDefinition.equals(MDSCHEMA_MEASUREGROUP_DIMENSIONS))
            return "63";
         if (rowsetDefinition.equals(DISCOVER_INSTANCES))
            return "1";
         if (rowsetDefinition.equals(MDSCHEMA_KPIS))
            return "63";
         if (rowsetDefinition.equals(DISCOVER_CONNECTIONS))
            return "127";
         if (rowsetDefinition.equals(DISCOVER_SESSIONS))
            return "511";
         if (rowsetDefinition.equals(DISCOVER_JOBS))
            return "31";
         if (rowsetDefinition.equals(DISCOVER_LOCKS))
            return "63";
         if (rowsetDefinition.equals(DISCOVER_XML_METADATA))
            return "8388607";
         if (rowsetDefinition.equals(DISCOVER_KEYWORDS))
            return "1";
         if (rowsetDefinition.equals(DISCOVER_ENUMERATORS))
            return "1";
         if (rowsetDefinition.equals(MDSCHEMA_INPUT_DATASOURCES))
            return "15";
         if (rowsetDefinition.equals(DISCOVER_TRANSACTIONS))
            return "3";
         if (rowsetDefinition.equals(DISCOVER_MASTER_KEY))
            return "1";
         if (rowsetDefinition.equals(DISCOVER_PERFORMANCE_COUNTERS))
            return "1";
         if (rowsetDefinition.equals(DISCOVER_PARTITION_DIMENSION_STAT))
            return "15";
         if (rowsetDefinition.equals(DISCOVER_PARTITION_DIMENSION_STAT))
            return "15";
         if (rowsetDefinition.equals(DISCOVER_PARTITION_STAT))
            return "15";
         if (rowsetDefinition.equals(DISCOVER_LOCATIONS))
            return "3";
         if (rowsetDefinition.equals(DISCOVER_DIMENSION_STAT))
            return "3";
         if (rowsetDefinition.equals(DISCOVER_COMMANDS))
            return "1";
         if (rowsetDefinition.equals(DISCOVER_COMMAND_OBJECTS))
            return "15";
         if (rowsetDefinition.equals(DISCOVER_OBJECT_ACTIVITY))
            return "3";
         if (rowsetDefinition.equals(DISCOVER_OBJECT_MEMORY_USAGE))
            return "3";
         if (rowsetDefinition.equals(DISCOVER_STORAGE_TABLES))
            return "7";
         if (rowsetDefinition.equals(DISCOVER_STORAGE_TABLE_COLUMNS))
            return "7";
         if (rowsetDefinition.equals(DISCOVER_STORAGE_TABLE_COLUMN_SEGMENTS))
            return "15";
         if (rowsetDefinition.equals(DISCOVER_CALC_DEPENDENCY))
            return "7";
         if (rowsetDefinition.equals(DISCOVER_CSDL_METADATA))
            return "7";
         if (rowsetDefinition.equals(DISCOVER_DB_CONNECTIONS))

            return "31";
         if (rowsetDefinition.equals(DISCOVER_MEMORYUSAGE))
            return "15";

         if (rowsetDefinition.equals(DISCOVER_MEMORYGRANT))
            return "1";
         if (rowsetDefinition.equals(DISCOVER_TRACE_DEFINITION_PROVIDERINFO))
            return "1";
         if (rowsetDefinition.equals(DISCOVER_XEVENT_TRACE_DEFINITION))
            return "1";
         if (rowsetDefinition.equals(DISCOVER_TRACE_COLUMNS))
            return "1";
         if (rowsetDefinition.equals(DISCOVER_TRACE_EVENT_CATEGORIES))
            return "1";
         if (rowsetDefinition.equals(DISCOVER_TRACES))
            return "3";

         if (rowsetDefinition.equals(DMSCHEMA_MINING_SERVICES))
            return "3";
         if (rowsetDefinition.equals(DMSCHEMA_MINING_SERVICE_PARAMETERS))
            return "3";
         if (rowsetDefinition.equals(DMSCHEMA_MINING_FUNCTIONS))
            return "3";
         if (rowsetDefinition.equals(DMSCHEMA_MINING_MODEL_CONTENT))
            return "1023";
         if (rowsetDefinition.equals(DMSCHEMA_MINING_MODEL_XML))
            return "15";
         if (rowsetDefinition.equals(DMSCHEMA_MINING_MODEL_CONTENT_PMML))
            return "15";
         if (rowsetDefinition.equals(DMSCHEMA_MINING_MODELS))
            return "127";
         if (rowsetDefinition.equals(DMSCHEMA_MINING_COLUMNS))
            return "15";
         if (rowsetDefinition.equals(DMSCHEMA_MINING_STRUCTURES))
            return "7";
         if (rowsetDefinition.equals(DMSCHEMA_MINING_STRUCTURE_COLUMNS))
            return "15";

         throw new IllegalArgumentException("illegal rowset input");
      }

      private String getSchemGuid(RowsetDefinition rowsetDefinition) {

         if (rowsetDefinition.equals(DBSCHEMA_CATALOGS))
            return "C8B52211-5CF3-11CE-ADE5-00AA0044773D";
         if (rowsetDefinition.equals(DBSCHEMA_TABLES))
            return "C8B52229-5CF3-11CE-ADE5-00AA0044773D";
         if (rowsetDefinition.equals(DBSCHEMA_COLUMNS))
            return "C8B52214-5CF3-11CE-ADE5-00AA0044773D";
         if (rowsetDefinition.equals(DBSCHEMA_PROVIDER_TYPES))
            return "C8B5222C-5CF3-11CE-ADE5-00AA0044773D";
         if (rowsetDefinition.equals(DISCOVER_DATASOURCES))
            return "06C03D41-F66D-49F3-B1B8-987F7AF4CF18";

         if (rowsetDefinition.equals(DISCOVER_LITERALS))
            return "C3EF5ECB-0A07-4665-A140-B075722DBDC2";
         if (rowsetDefinition.equals(DISCOVER_PROPERTIES))
            return "4B40ADFB-8B09-4758-97BB-636E8AE97BCF";
         if (rowsetDefinition.equals(DISCOVER_SCHEMA_ROWSETS))
            return "EEA0302B-7922-4992-8991-0E605D0E5593";
         if (rowsetDefinition.equals(MDSCHEMA_ACTIONS))
            return "A07CCD08-8148-11D0-87BB-00C04FC33942";
         if (rowsetDefinition.equals(MDSCHEMA_CUBES))
            return "C8B522D8-5CF3-11CE-ADE5-00AA0044773D";
         if (rowsetDefinition.equals(MDSCHEMA_DIMENSIONS))
            return "C8B522D9-5CF3-11CE-ADE5-00AA0044773D";
         if (rowsetDefinition.equals(MDSCHEMA_FUNCTIONS))
            return "A07CCD07-8148-11D0-87BB-00C04FC33942";
         if (rowsetDefinition.equals(MDSCHEMA_HIERARCHIES))
            return "C8B522DA-5CF3-11CE-ADE5-00AA0044773D";
         if (rowsetDefinition.equals(MDSCHEMA_LEVELS))
            return "C8B522DB-5CF3-11CE-ADE5-00AA0044773D";
         if (rowsetDefinition.equals(MDSCHEMA_MEASURES))
            return "C8B522DC-5CF3-11CE-ADE5-00AA0044773D";
         if (rowsetDefinition.equals(MDSCHEMA_MEMBERS))
            return "C8B522DE-5CF3-11CE-ADE5-00AA0044773D";
         if (rowsetDefinition.equals(MDSCHEMA_PROPERTIES))
            return "C8B522DD-5CF3-11CE-ADE5-00AA0044773D";
         if (rowsetDefinition.equals(MDSCHEMA_SETS))
            return "A07CCD0B-8148-11D0-87BB-00C04FC33942";
         if (rowsetDefinition.equals(MDSCHEMA_MEASUREGROUPS))
            return "E1625EBF-FA96-42FD-BEA6-DB90ADAFD96B";
         if (rowsetDefinition.equals(MDSCHEMA_MEASUREGROUP_DIMENSIONS))
            return "A07CCD33-8148-11D0-87BB-00C04FC33942";
         if (rowsetDefinition.equals(DISCOVER_INSTANCES))
            return "20518699-2474-4C15-9885-0E947EC7A7E3";
         if (rowsetDefinition.equals(MDSCHEMA_KPIS))
            return "2AE44109-ED3D-4842-B16F-B694D1CB0E3F";
         if (rowsetDefinition.equals(DISCOVER_CONNECTIONS))
            return "A07CCD25-8148-11D0-87BB-00C04FC33942";
         if (rowsetDefinition.equals(DISCOVER_SESSIONS))
            return "A07CCD26-8148-11D0-87BB-00C04FC33942";
         if (rowsetDefinition.equals(DISCOVER_JOBS))
            return "A07CCD27-8148-11D0-87BB-00C04FC33942";
         if (rowsetDefinition.equals(DISCOVER_LOCKS))
            return "A07CCD24-8148-11D0-87BB-00C04FC33942";
         if (rowsetDefinition.equals(DISCOVER_XML_METADATA))
            return "3444B255-171E-4CB9-AD98-19E57888A75F";
         if (rowsetDefinition.equals(DISCOVER_KEYWORDS))
            return "1426C443-4CDD-4A40-8F45-572FAB9BBAA1";
         if (rowsetDefinition.equals(DISCOVER_ENUMERATORS))
            return "55A9E78B-ACCB-45B4-95A6-94C5065617A7";
         if (rowsetDefinition.equals(MDSCHEMA_INPUT_DATASOURCES))
            return "A07CCD32-8148-11D0-87BB-00C04FC33942";
         if (rowsetDefinition.equals(DISCOVER_TRANSACTIONS))
            return "A07CCD28-8148-11D0-87BB-00C04FC33942";
         if (rowsetDefinition.equals(DISCOVER_MASTER_KEY))
            return "A07CCD29-8148-11D0-87BB-00C04FC33942";
         if (rowsetDefinition.equals(DISCOVER_PERFORMANCE_COUNTERS))
            return "A07CCD2E-8148-11D0-87BB-00C04FC33942";
         if (rowsetDefinition.equals(DISCOVER_PARTITION_DIMENSION_STAT))
            return "A07CCD8E-8148-11D0-87BB-00C04FC33942";
         if (rowsetDefinition.equals(DISCOVER_PARTITION_STAT))
            return "A07CCD8F-8148-11D0-87BB-00C04FC33942";
         if (rowsetDefinition.equals(DISCOVER_LOCATIONS))
            return "A07CCD92-8148-11D0-87BB-00C04FC33942";
         if (rowsetDefinition.equals(DISCOVER_DIMENSION_STAT))
            return "A07CCD90-8148-11D0-87BB-00C04FC33942";
         if (rowsetDefinition.equals(DISCOVER_COMMANDS))
            return "A07CCD34-8148-11D0-87BB-00C04FC33942";
         if (rowsetDefinition.equals(DISCOVER_COMMAND_OBJECTS))
            return "A07CCD35-8148-11D0-87BB-00C04FC33942";
         if (rowsetDefinition.equals(DISCOVER_OBJECT_ACTIVITY))
            return "A07CCD36-8148-11D0-87BB-00C04FC33942";
         if (rowsetDefinition.equals(DISCOVER_OBJECT_MEMORY_USAGE))
            return "A07CCD37-8148-11D0-87BB-00C04FC33942";
         if (rowsetDefinition.equals(DISCOVER_STORAGE_TABLES))
            return "A07CCD43-8148-11D0-87BB-00C04FC33942";
         if (rowsetDefinition.equals(DISCOVER_STORAGE_TABLE_COLUMNS))
            return "A07CCD44-8148-11D0-87BB-00C04FC33942";
         if (rowsetDefinition.equals(DISCOVER_STORAGE_TABLE_COLUMN_SEGMENTS))
            return "A07CCD45-8148-11D0-87BB-00C04FC33942";
         if (rowsetDefinition.equals(DISCOVER_CALC_DEPENDENCY))
            return "A07CCD46-8148-11D0-87BB-00C04FC33942";
         if (rowsetDefinition.equals(DISCOVER_CSDL_METADATA))
            return "87B86062-21C3-460F-B4F8-5BE98394F13B";
         if (rowsetDefinition.equals(DISCOVER_DB_CONNECTIONS))
            return "A07CCD2A-8148-11D0-87BB-00C04FC33942";
         if (rowsetDefinition.equals(DISCOVER_MEMORYUSAGE))
            return "A07CCD21-8148-11D0-87BB-00C04FC33942";

         if (rowsetDefinition.equals(DISCOVER_MEMORYGRANT))
            return "A07CCD23-8148-11D0-87BB-00C04FC33942";
         if (rowsetDefinition.equals(DISCOVER_TRACE_DEFINITION_PROVIDERINFO))
            return "A07CCD1B-8148-11D0-87BB-00C04FC33942";
         if (rowsetDefinition.equals(DISCOVER_XEVENT_TRACE_DEFINITION))
            return "A07CCD1C-8148-11D0-87BB-00C04FC33942";
         if (rowsetDefinition.equals(DISCOVER_TRACE_COLUMNS))
            return "A07CCD18-8148-11D0-87BB-00C04FC33942";
         if (rowsetDefinition.equals(DISCOVER_TRACE_EVENT_CATEGORIES))
            return "A07CCD19-8148-11D0-87BB-00C04FC33942";
         if (rowsetDefinition.equals(DISCOVER_TRACES))
            return "A07CCD1A-8148-11D0-87BB-00C04FC33942";

         if (rowsetDefinition.equals(DMSCHEMA_MINING_SERVICES))
            return "3ADD8A95-D8B9-11D2-8D2A-00E029154FDE";
         if (rowsetDefinition.equals(DMSCHEMA_MINING_SERVICE_PARAMETERS))
            return "3ADD8A75-D8B9-11D2-8D2A-00E029154FDE";
         if (rowsetDefinition.equals(DMSCHEMA_MINING_FUNCTIONS))
            return "3ADD8A79-D8B9-11D2-8D2A-00E029154FDE";
         if (rowsetDefinition.equals(DMSCHEMA_MINING_MODEL_CONTENT))
            return "3ADD8A76-D8B9-11D2-8D2A-00E029154FDE";
         if (rowsetDefinition.equals(DMSCHEMA_MINING_MODEL_XML))
            return "4290B2D5-0E9C-4AA7-9369-98C95CFD9D13";
         if (rowsetDefinition.equals(DMSCHEMA_MINING_MODEL_CONTENT_PMML))
            return "4290B2D5-0E9C-4AA7-9369-98C95CFD9D13";
         if (rowsetDefinition.equals(DMSCHEMA_MINING_MODELS))
            return "3ADD8A77-D8B9-11D2-8D2A-00E029154FDE";
         if (rowsetDefinition.equals(DMSCHEMA_MINING_COLUMNS))
            return "3ADD8A78-D8B9-11D2-8D2A-00E029154FDE";
         if (rowsetDefinition.equals(DMSCHEMA_MINING_STRUCTURES))
            return "883269F3-0CAD-462F-B6F5-E88A72418C4B";
         if (rowsetDefinition.equals(DMSCHEMA_MINING_STRUCTURE_COLUMNS))
            return "9952E836-BFBF-4D1F-8535-9B67DBD9DDFE";

         throw new IllegalArgumentException("illegal rowset input");
      }

      protected void setProperty(PropertyDefinition propertyDef, String value) {
         switch (propertyDef) {
         case Content:
            break;
         default:
            super.setProperty(propertyDef, value);
         }
      }
   }

   public String getDescription() {
      return description;
   }

   static class DiscoverPropertiesRowset extends Rowset {
      private final Util.Functor1<Boolean, PropertyDefinition> propNameCond;

      DiscoverPropertiesRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(DISCOVER_PROPERTIES, request, handler);
         propNameCond = makeCondition(PROPDEF_NAME_GETTER, PropertyName);
      }

      private static final Column PropertyName = new Column("PropertyName", Type.StringSometimesArray, null, Column.RESTRICTION, Column.REQUIRED, "The name of the property.");
      private static final Column PropertyDescription = new Column("PropertyDescription", Type.String, null, Column.NOT_RESTRICTION, Column.REQUIRED,
               "A localizable text description of the property.");
      private static final Column PropertyType = new Column("PropertyType", Type.String, null, Column.NOT_RESTRICTION, Column.REQUIRED, "The XML data type of the property.");
      private static final Column PropertyAccessType = new Column("PropertyAccessType", Type.EnumString, Enumeration.ACCESS, Column.NOT_RESTRICTION, Column.REQUIRED,
               "Access for the property. The value can be Read, Write, or " + "ReadWrite.");
      private static final Column IsRequired = new Column("IsRequired", Type.Boolean, null, Column.NOT_RESTRICTION, Column.REQUIRED,
               "True if a property is required, false if it is not required.");
      private static final Column Value = new Column("Value", Type.String, null, Column.NOT_RESTRICTION, Column.REQUIRED, "The current value of the property.");

      protected boolean needConnection() {
         return false;
      }

      public void populateImpl(XmlaResponse response, OlapConnection connection, List<Row> rows) throws XmlaException {
         for (PropertyDefinition propertyDefinition : PropertyDefinition.class.getEnumConstants()) {
            if (!propNameCond.apply(propertyDefinition)) {
               continue;
            }
            Row row = new Row();
            row.set(PropertyName.name, propertyDefinition.name());
            row.set(PropertyDescription.name, propertyDefinition.description);
            row.set(PropertyType.name, propertyDefinition.type.getName());
            row.set(PropertyAccessType.name, propertyDefinition.access);
            row.set(IsRequired.name, false);
            row.set(Value.name, propertyDefinition.value);
            addRow(row, rows);
         }
      }

      protected void setProperty(PropertyDefinition propertyDef, String value) {
         switch (propertyDef) {
         case Content:
            break;
         default:
            super.setProperty(propertyDef, value);
         }
      }
   }

   static class DiscoverInstancesRowset extends Rowset {
      DiscoverInstancesRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(DISCOVER_INSTANCES, request, handler);
      }

      private static final Column InstanceName = new Column("INSTANCE_NAME", Type.String, null, Column.RESTRICTION, Column.REQUIRED,
               "The name of the literal described in the row.\n" + "Example: DBLITERAL_LIKE_PERCENT");

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {

      }
   }

   static class DiscoverMemoryUsage extends Rowset {
      DiscoverMemoryUsage(XmlaRequest request, CustomXmlaHandler handler) {
         super(DISCOVER_MEMORYUSAGE, request, handler);
      }

      private static final Column SPID = new Column("SPID", Type.UnsignedInteger, null, Column.RESTRICTION, Column.OPTIONAL, "");
      private static final Column MemoryUsed = new Column("MemoryUsed", Type.Long, null, Column.RESTRICTION, Column.OPTIONAL, "");
      private static final Column BaseObjectType = new Column("BaseObjectType", Type.UnsignedInteger, null, Column.RESTRICTION, Column.OPTIONAL, "");
      private static final Column Shrinkable = new Column("Shrinkable", Type.Boolean, null, Column.RESTRICTION, Column.OPTIONAL, "");

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {

      }
   }

   static class DiscoveryMemoryGrant extends Rowset {
      DiscoveryMemoryGrant(XmlaRequest request, CustomXmlaHandler handler) {
         super(DISCOVER_MEMORYGRANT, request, handler);
      }

      private static final Column SPID = new Column("SPID", Type.String, null, Column.RESTRICTION, Column.OPTIONAL, "");

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {

      }
   }

   // DiscoverTracesRowset

   static class DiscoverTraceDefinitionProviderInfo extends Rowset {
      DiscoverTraceDefinitionProviderInfo(XmlaRequest request, CustomXmlaHandler handler) {
         super(DISCOVER_TRACE_DEFINITION_PROVIDERINFO, request, handler);
      }

      private static final Column Data = new Column("Data", Type.String, null, Column.RESTRICTION, Column.OPTIONAL, "");

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {

      }
   }

   static class DiscoverTracesRowset extends Rowset {
      DiscoverTracesRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(DISCOVER_TRACES, request, handler);
      }

      private static final Column TraceId = new Column("TraceID", RowsetDefinition.Type.String, null, Column.RESTRICTION, Column.OPTIONAL, "");
      private static final Column Type = new Column("Type", RowsetDefinition.Type.String, null, Column.RESTRICTION, Column.OPTIONAL, "");

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {

      }
   }

   static class DiscoverXeventTraceDefinition extends Rowset {
      DiscoverXeventTraceDefinition(XmlaRequest request, CustomXmlaHandler handler) {
         super(DISCOVER_XEVENT_TRACE_DEFINITION, request, handler);
      }

      private static final Column Data = new Column("Data", Type.String, null, Column.RESTRICTION, Column.OPTIONAL, "");

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {

      }
   }

   static class DiscoverTraceColumns extends Rowset {
      DiscoverTraceColumns(XmlaRequest request, CustomXmlaHandler handler) {
         super(DISCOVER_TRACE_COLUMNS, request, handler);
      }

      private static final Column Data = new Column("Data", Type.String, null, Column.RESTRICTION, Column.OPTIONAL, "");

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {

      }
   }

   static class DiscoverTraceEventCategories extends Rowset {
      DiscoverTraceEventCategories(XmlaRequest request, CustomXmlaHandler handler) {
         super(DISCOVER_TRACE_EVENT_CATEGORIES, request, handler);
      }

      private static final Column Data = new Column("Data", Type.String, null, Column.RESTRICTION, Column.OPTIONAL, "");

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {

      }
   }

   static class DiscoverLiteralsRowset extends Rowset {
      DiscoverLiteralsRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(DISCOVER_LITERALS, request, handler);
      }

      public static List getColumnNamese() {
         String[] names = new String[6];
         names[0] = LiteralName.name;
         names[1] = LiteralValue.name;
         names[2] = LiteralInvalidChars.name;
         names[3] = LiteralInvalidStartingChars.name;
         names[4] = LiteralMaxLength.name;
         names[5] = LiteralNameEnumValue.name;
         return Arrays.asList(names);
      }

      private static final Column LiteralName = new Column("LiteralName", Type.StringSometimesArray, null, Column.RESTRICTION, Column.REQUIRED,
               "The name of the literal described in the row.\n" + "Example: DBLITERAL_LIKE_PERCENT");

      private static final Column LiteralValue = new Column("LiteralValue", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "Contains the actual literal value.\n"
               + "Example, if LiteralName is DBLITERAL_LIKE_PERCENT and the " + "percent character (%) is used to match zero or more characters "
               + "in a LIKE clause, this column's value would be \"%\".");

      private static final Column LiteralInvalidChars = new Column("LiteralInvalidChars", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "The characters, in the literal, that are not valid.\n" + "For example, if table names can contain anything other than a "
                        + "numeric character, this string would be \"0123456789\".");

      private static final Column LiteralInvalidStartingChars = new Column("LiteralInvalidStartingChars", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "The characters that are not valid as the first character of the " + "literal. If the literal can start with any valid character, " + "this is null.");

      private static final Column LiteralMaxLength = new Column("LiteralMaxLength", Type.Integer, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "The maximum number of characters in the literal. If there is no " + "maximum or the maximum is unknown, the value is ?1.");
      private static final Column LiteralNameEnumValue = new Column("LiteralNameEnumValue", Type.Integer, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");

      public void populateImpl(XmlaResponse response, OlapConnection connection, List<Row> rows) throws XmlaException {
         populate(XmlaConstants.Literal.class, rows, new Comparator<XmlaConstants.Literal>() {
            public int compare(XmlaConstants.Literal o1, XmlaConstants.Literal o2) {
               return o1.name().compareTo(o2.name());
            }
         });
      }

      protected void setProperty(PropertyDefinition propertyDef, String value) {
         switch (propertyDef) {
         case Content:
            break;
         default:
            super.setProperty(propertyDef, value);
         }
      }
   }

   static class DbschemaCatalogsRowset extends Rowset {
      DbschemaCatalogsRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(DBSCHEMA_CATALOGS, request, handler);
      }

      private static final Column CatalogName = new Column("CATALOG_NAME", Type.String, null, Column.RESTRICTION, Column.REQUIRED, "Catalog name. Cannot be NULL.");
      private static final Column Description = new Column("DESCRIPTION", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "Human-readable description of the catalog.");
      private static final Column Roles = new Column("ROLES", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "A comma delimited list of roles to which the current user " + "belongs. An asterisk (*) is included as a role if the "
                        + "current user is a server or database administrator. " + "Username is appended to ROLES if one of the roles uses " + "dynamic security.");
      private static final Column DateModified = new Column("DATE_MODIFIED", Type.DateTime, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "The date that the catalog was last modified.");

      public void populateImpl(XmlaResponse response, OlapConnection connection, List<Row> rows) throws XmlaException, SQLException {
         Iterable<Catalog> catalogsItr = catIter(connection);

         for (Catalog catalog : catalogsItr) {

            for (@SuppressWarnings("unused")
            Schema m : catalog.getSchemas()) {
               Row row = new Row();
               row.set(CatalogName.name, catalog.getName());
               row.set(Description.name, "No description available");
               addRow(row, rows);
            }
         }
      }

      protected void setProperty(PropertyDefinition propertyDef, String value) {
         switch (propertyDef) {
         case Content:
            break;
         default:
            super.setProperty(propertyDef, value);
         }
      }
   }

   static class DbschemaColumnsRowset extends Rowset {
      private final Util.Functor1<Boolean, Catalog> tableCatalogCond;
      private final Util.Functor1<Boolean, Cube> tableNameCond;
      private final Util.Functor1<Boolean, String> columnNameCond;

      DbschemaColumnsRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(DBSCHEMA_COLUMNS, request, handler);
         tableCatalogCond = makeCondition(CATALOG_NAME_GETTER, TableCatalog);
         tableNameCond = makeCondition(ELEMENT_NAME_GETTER, TableName);
         columnNameCond = makeCondition(ColumnName);
      }

      private static final Column TableCatalog = new Column("TABLE_CATALOG", Type.String, null, Column.RESTRICTION, Column.REQUIRED, "The name of the Database.");
      private static final Column TableSchema = new Column("TABLE_SCHEMA", Type.String, null, Column.RESTRICTION, Column.OPTIONAL, null);
      private static final Column ColumnOlapType = new Column("COLUMN_OLAP_TYPE", Type.String, null, Column.RESTRICTION, Column.OPTIONAL, null);

      private static final Column TableName = new Column("TABLE_NAME", Type.String, null, Column.RESTRICTION, Column.REQUIRED, "The name of the cube.");
      private static final Column ColumnName = new Column("COLUMN_NAME", Type.String, null, Column.RESTRICTION, Column.REQUIRED, "The name of the attribute hierarchy or measure.");
      private static final Column OrdinalPosition = new Column("ORDINAL_POSITION", Type.UnsignedInteger, null, Column.NOT_RESTRICTION, Column.REQUIRED,
               "The position of the column, beginning with 1.");
      private static final Column ColumnHasDefault = new Column("COLUMN_HAS_DEFAULT", Type.Boolean, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "Not supported.");
      /*
       * A bitmask indicating the information stored in DBCOLUMNFLAGS in OLE DB.
       * 1 = Bookmark 2 = Fixed length 4 = Nullable 8 = Row versioning 16 =
       * Updateable column
       * 
       * And, of course, MS SQL Server sometimes has the value of 80!!
       */
      private static final Column ColumnFlags = new Column("COLUMN_FLAGS", Type.UnsignedInteger, null, Column.NOT_RESTRICTION, Column.REQUIRED,
               "A DBCOLUMNFLAGS bitmask indicating column properties.");
      private static final Column IsNullable = new Column("IS_NULLABLE", Type.Boolean, null, Column.NOT_RESTRICTION, Column.REQUIRED, "Always returns false.");
      private static final Column DataType = new Column("DATA_TYPE", Type.UnsignedShort, null, Column.NOT_RESTRICTION, Column.REQUIRED,
               "The data type of the column. Returns a string for dimension " + "columns and a variant for measures.");
      private static final Column NumericPrecision = new Column("NUMERIC_PRECISION", Type.UnsignedShort, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "The maximum precision of the column for numeric data types " + "other than DBTYPE_VARNUMERIC.");
      private static final Column CharacterMaximumLength = new Column("CHARACTER_MAXIMUM_LENGTH", Type.UnsignedInteger, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "The maximum possible length of a value within the column.");
      private static final Column CharacterOctetLength = new Column("CHARACTER_OCTET_LENGTH", Type.UnsignedInteger, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "The maximum possible length of a value within the column, in " + "bytes, for character or binary columns.");

      private static final Column NumericScale = new Column("NUMERIC_SCALE", Type.Short, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "The number of digits to the right of the decimal point for " + "DBTYPE_DECIMAL, DBTYPE_NUMERIC, DBTYPE_VARNUMERIC. " + "Otherwise, this is NULL.");

      private final static Long restrictionsMask = 31L;
      private final static String schemaCuid = "c8b52214-5cf3-11ce-ade5-00aa0044773d";

      public void populateImpl(XmlaResponse response, OlapConnection connection, List<Row> rows) throws XmlaException, OlapException {
         for (Catalog catalog : catIter(connection, catNameCond(), tableCatalogCond)) {
            // By definition, mondrian catalogs have only one
            // schema. It is safe to use get(0)
            final Schema schema = catalog.getSchemas().get(0);
            final boolean emitInvisibleMembers = XmlaUtil.shouldEmitInvisibleMembers(request);
            int ordinalPosition = 1;
            Row row;

            for (Cube cube : filter(sortedCubes(schema), tableNameCond)) {
               for (Dimension dimension : cube.getDimensions()) {
                  for (Hierarchy hierarchy : dimension.getHierarchies()) {
                     ordinalPosition = populateHierarchy(cube, hierarchy, ordinalPosition, rows);
                  }
               }

               List<Measure> rms = cube.getMeasures();
               for (int k = 1; k < rms.size(); k++) {
                  Measure member = rms.get(k);

                  // null == true for regular cubes
                  // virtual cubes do not set the visible property
                  // on its measures so it might be null.
                  Boolean visible = (Boolean) member.getPropertyValue(Property.StandardMemberProperty.$visible);
                  if (visible == null) {
                     visible = true;
                  }
                  if (!emitInvisibleMembers && !visible) {
                     continue;
                  }

                  String memberName = member.getName();
                  final String columnName = "Measures:" + memberName;
                  if (!columnNameCond.apply(columnName)) {
                     continue;
                  }

                  row = new Row();
                  row.set(TableCatalog.name, catalog.getName());
                  row.set(TableName.name, cube.getName());
                  row.set(ColumnName.name, columnName);
                  row.set(OrdinalPosition.name, ordinalPosition++);
                  row.set(ColumnHasDefault.name, false);
                  row.set(ColumnFlags.name, 0);
                  row.set(IsNullable.name, false);
                  // here is where one tries to determine the
                  // type of the column - since these are all
                  // Measures, aggregate Measures??, maybe they
                  // are all numeric? (or currency)
                  row.set(DataType.name, XmlaConstants.DBType.R8.xmlaOrdinal());
                  // 16/255 seems to be what MS SQL Server
                  // always returns.
                  row.set(NumericPrecision.name, 16);
                  row.set(NumericScale.name, 255);
                  addRow(row, rows);
               }
            }
         }
      }

      private int populateHierarchy(Cube cube, Hierarchy hierarchy, int ordinalPosition, List<Row> rows) {
         String schemaName = cube.getSchema().getName();
         String cubeName = cube.getName();
         String hierarchyName = hierarchy.getName();

         if (hierarchy.hasAll()) {
            Row row = new Row();
            row.set(TableCatalog.name, schemaName);
            row.set(TableName.name, cubeName);
            row.set(ColumnName.name, hierarchyName + ":(All)!NAME");
            row.set(OrdinalPosition.name, ordinalPosition++);
            row.set(ColumnHasDefault.name, false);
            row.set(ColumnFlags.name, 0);
            row.set(IsNullable.name, false);
            // names are always WSTR
            row.set(DataType.name, XmlaConstants.DBType.WSTR.xmlaOrdinal());
            row.set(CharacterMaximumLength.name, 0);
            row.set(CharacterOctetLength.name, 0);
            addRow(row, rows);

            row = new Row();
            row.set(TableCatalog.name, schemaName);
            row.set(TableName.name, cubeName);
            row.set(ColumnName.name, hierarchyName + ":(All)!UNIQUE_NAME");
            row.set(OrdinalPosition.name, ordinalPosition++);
            row.set(ColumnHasDefault.name, false);
            row.set(ColumnFlags.name, 0);
            row.set(IsNullable.name, false);
            // names are always WSTR
            row.set(DataType.name, XmlaConstants.DBType.WSTR.xmlaOrdinal());
            row.set(CharacterMaximumLength.name, 0);
            row.set(CharacterOctetLength.name, 0);
            addRow(row, rows);

         }

         for (Level level : hierarchy.getLevels()) {
            ordinalPosition = populateLevel(cube, hierarchy, level, ordinalPosition, rows);
         }
         return ordinalPosition;
      }

      private int populateLevel(Cube cube, Hierarchy hierarchy, Level level, int ordinalPosition, List<Row> rows) {
         String schemaName = cube.getSchema().getName();
         String cubeName = cube.getName();
         String hierarchyName = hierarchy.getName();
         String levelName = level.getName();

         Row row = new Row();
         row.set(TableCatalog.name, schemaName);
         row.set(TableName.name, cubeName);
         row.set(ColumnName.name, hierarchyName + ':' + levelName + "!NAME");
         row.set(OrdinalPosition.name, ordinalPosition++);
         row.set(ColumnHasDefault.name, false);
         row.set(ColumnFlags.name, 0);
         row.set(IsNullable.name, false);
         // names are always WSTR
         row.set(DataType.name, XmlaConstants.DBType.WSTR.xmlaOrdinal());
         row.set(CharacterMaximumLength.name, 0);
         row.set(CharacterOctetLength.name, 0);
         addRow(row, rows);

         row = new Row();
         row.set(TableCatalog.name, schemaName);
         row.set(TableName.name, cubeName);
         row.set(ColumnName.name, hierarchyName + ':' + levelName + "!UNIQUE_NAME");
         row.set(OrdinalPosition.name, ordinalPosition++);
         row.set(ColumnHasDefault.name, false);
         row.set(ColumnFlags.name, 0);
         row.set(IsNullable.name, false);
         // names are always WSTR
         row.set(DataType.name, XmlaConstants.DBType.WSTR.xmlaOrdinal());
         row.set(CharacterMaximumLength.name, 0);
         row.set(CharacterOctetLength.name, 0);
         addRow(row, rows);

         NamedList<Property> props = level.getProperties();
         for (Property prop : props) {
            String propName = prop.getName();

            row = new Row();
            row.set(TableCatalog.name, schemaName);
            row.set(TableName.name, cubeName);
            row.set(ColumnName.name, hierarchyName + ':' + levelName + '!' + propName);
            row.set(OrdinalPosition.name, ordinalPosition++);
            row.set(ColumnHasDefault.name, false);
            row.set(ColumnFlags.name, 0);
            row.set(IsNullable.name, false);

            XmlaConstants.DBType dbType = getDBTypeFromProperty(prop);
            row.set(DataType.name, dbType.xmlaOrdinal());

            switch (prop.getDatatype()) {
            case STRING:
               row.set(CharacterMaximumLength.name, 0);
               row.set(CharacterOctetLength.name, 0);
               break;
            case INTEGER:
            case UNSIGNED_INTEGER:
            case DOUBLE:
               // 16/255 seems to be what MS SQL Server
               // always returns.
               row.set(NumericPrecision.name, 16);
               row.set(NumericScale.name, 255);
               break;
            case BOOLEAN:
               row.set(NumericPrecision.name, 255);
               row.set(NumericScale.name, 255);
               break;
            default:
               // what type is it really, its
               // not a string
               row.set(CharacterMaximumLength.name, 0);
               row.set(CharacterOctetLength.name, 0);
               break;
            }
            addRow(row, rows);
         }
         return ordinalPosition;
      }

      protected void setProperty(PropertyDefinition propertyDef, String value) {
         switch (propertyDef) {
         case Content:
            break;
         default:
            super.setProperty(propertyDef, value);
         }
      }

      public static Long getRestrictionsmask() {
         return restrictionsMask;
      }

      public static String getSchemacuid() {
         return schemaCuid;
      }
   }

   static class DbschemaProviderTypesRowset extends Rowset {
      private final Util.Functor1<Boolean, Integer> dataTypeCond;

      DbschemaProviderTypesRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(DBSCHEMA_PROVIDER_TYPES, request, handler);
         dataTypeCond = makeCondition(DataType);
      }

      /*
       * DATA_TYPE DBTYPE_UI2 BEST_MATCH DBTYPE_BOOL Column(String name, Type
       * type, Enumeration enumeratedType, boolean restriction, boolean
       * nullable, String description)
       */
      /*
       * These are the columns returned by SQL Server.
       */
      private static final Column TypeName = new Column("TYPE_NAME", Type.String, null, Column.NOT_RESTRICTION, Column.REQUIRED, "The provider-specific data type name.");
      private static final Column DataType = new Column("DATA_TYPE", Type.UnsignedShort, null, Column.RESTRICTION, Column.REQUIRED, "The indicator of the data type.");
      private static final Column ColumnSize = new Column("COLUMN_SIZE", Type.UnsignedInteger, null, Column.NOT_RESTRICTION, Column.REQUIRED,
               "The length of a non-numeric column. If the data type is " + "numeric, this is the upper bound on the maximum precision " + "of the data type.");
      private static final Column LiteralPrefix = new Column("LITERAL_PREFIX", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "The character or characters used to prefix a literal of this " + "type in a text command.");
      private static final Column LiteralSuffix = new Column("LITERAL_SUFFIX", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "The character or characters used to suffix a literal of this " + "type in a text command.");
      private static final Column IsNullable = new Column("IS_NULLABLE", Type.Boolean, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "A Boolean that indicates whether the data type is nullable. " + "NULL-- indicates that it is not known whether the data type " + "is nullable.");
      private static final Column CaseSensitive = new Column("CASE_SENSITIVE", Type.Boolean, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "A Boolean that indicates whether the data type is a " + "characters type and case-sensitive.");
      private static final Column Searchable = new Column("SEARCHABLE", Type.UnsignedInteger, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "An integer indicating how the data type can be used in " + "searches if the provider supports ICommandText; otherwise, " + "NULL.");
      private static final Column UnsignedAttribute = new Column("UNSIGNED_ATTRIBUTE", Type.Boolean, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "A Boolean that indicates whether the data type is unsigned.");
      private static final Column FixedPrecScale = new Column("FIXED_PREC_SCALE", Type.Boolean, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "A Boolean that indicates whether the data type has a fixed " + "precision and scale.");
      private static final Column AutoUniqueValue = new Column("AUTO_UNIQUE_VALUE", Type.Boolean, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "A Boolean that indicates whether the data type is " + "autoincrementing.");
      private static final Column IsLong = new Column("IS_LONG", Type.Boolean, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "A Boolean that indicates whether the data type is a binary " + "large object (BLOB) and has very long data.");
      private static final Column BestMatch = new Column("BEST_MATCH", Type.Boolean, null, Column.RESTRICTION, Column.OPTIONAL,
               "A Boolean that indicates whether the data type is a best " + "match.");

      @Override
      protected boolean needConnection() {
         return false;
      }

      public void populateImpl(XmlaResponse response, OlapConnection connection, List<Row> rows) throws XmlaException {
         // Identifies the (base) data types supported by the data provider.
         Row row;

         // i4
         Integer dt = XmlaConstants.DBType.I4.xmlaOrdinal();
         if (dataTypeCond.apply(dt)) {
            row = new Row();
            row.set(TypeName.name, XmlaConstants.DBType.I4.userName);
            row.set(DataType.name, dt);
            row.set(ColumnSize.name, 8);
            row.set(IsNullable.name, true);
            row.set(Searchable.name, null);
            row.set(UnsignedAttribute.name, false);
            row.set(FixedPrecScale.name, false);
            row.set(AutoUniqueValue.name, false);
            row.set(IsLong.name, false);
            row.set(BestMatch.name, true);
            addRow(row, rows);
         }

         // R8
         dt = XmlaConstants.DBType.R8.xmlaOrdinal();
         if (dataTypeCond.apply(dt)) {
            row = new Row();
            row.set(TypeName.name, XmlaConstants.DBType.R8.userName);
            row.set(DataType.name, dt);
            row.set(ColumnSize.name, 16);
            row.set(IsNullable.name, true);
            row.set(Searchable.name, null);
            row.set(UnsignedAttribute.name, false);
            row.set(FixedPrecScale.name, false);
            row.set(AutoUniqueValue.name, false);
            row.set(IsLong.name, false);
            row.set(BestMatch.name, true);
            addRow(row, rows);
         }

         // CY
         dt = XmlaConstants.DBType.CY.xmlaOrdinal();
         if (dataTypeCond.apply(dt)) {
            row = new Row();
            row.set(TypeName.name, XmlaConstants.DBType.CY.userName);
            row.set(DataType.name, dt);
            row.set(ColumnSize.name, 8);
            row.set(IsNullable.name, true);
            row.set(Searchable.name, null);
            row.set(UnsignedAttribute.name, false);
            row.set(FixedPrecScale.name, false);
            row.set(AutoUniqueValue.name, false);
            row.set(IsLong.name, false);
            row.set(BestMatch.name, true);
            addRow(row, rows);
         }

         // BOOL
         dt = XmlaConstants.DBType.BOOL.xmlaOrdinal();
         if (dataTypeCond.apply(dt)) {
            row = new Row();
            row.set(TypeName.name, XmlaConstants.DBType.BOOL.userName);
            row.set(DataType.name, dt);
            row.set(ColumnSize.name, 1);
            row.set(IsNullable.name, true);
            row.set(Searchable.name, null);
            row.set(UnsignedAttribute.name, false);
            row.set(FixedPrecScale.name, false);
            row.set(AutoUniqueValue.name, false);
            row.set(IsLong.name, false);
            row.set(BestMatch.name, true);
            addRow(row, rows);
         }

         // I8
         dt = XmlaConstants.DBType.I8.xmlaOrdinal();
         if (dataTypeCond.apply(dt)) {
            row = new Row();
            row.set(TypeName.name, XmlaConstants.DBType.I8.userName);
            row.set(DataType.name, dt);
            row.set(ColumnSize.name, 16);
            row.set(IsNullable.name, true);
            row.set(Searchable.name, null);
            row.set(UnsignedAttribute.name, false);
            row.set(FixedPrecScale.name, false);
            row.set(AutoUniqueValue.name, false);
            row.set(IsLong.name, false);
            row.set(BestMatch.name, true);
            addRow(row, rows);
         }

         // WSTR
         dt = XmlaConstants.DBType.WSTR.xmlaOrdinal();
         if (dataTypeCond.apply(dt)) {
            row = new Row();
            row.set(TypeName.name, XmlaConstants.DBType.WSTR.userName);
            row.set(DataType.name, dt);
            // how big are the string columns in the db
            row.set(ColumnSize.name, 255);
            row.set(LiteralPrefix.name, "\"");
            row.set(LiteralSuffix.name, "\"");
            row.set(IsNullable.name, true);
            row.set(CaseSensitive.name, false);
            row.set(Searchable.name, null);
            row.set(FixedPrecScale.name, false);
            row.set(AutoUniqueValue.name, false);
            row.set(IsLong.name, false);
            row.set(BestMatch.name, true);
            addRow(row, rows);
         }
      }

      protected void setProperty(PropertyDefinition propertyDef, String value) {
         switch (propertyDef) {
         case Content:
            break;
         default:
            super.setProperty(propertyDef, value);
         }
      }
   }

   static class DbschemaTablesRowset extends Rowset {
      private final Util.Functor1<Boolean, Catalog> tableCatalogCond;
      private final Util.Functor1<Boolean, Cube> tableNameCond;
      private final Util.Functor1<Boolean, String> tableTypeCond;

      DbschemaTablesRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(DBSCHEMA_TABLES, request, handler);
         tableCatalogCond = makeCondition(CATALOG_NAME_GETTER, TableCatalog);
         tableNameCond = makeCondition(ELEMENT_NAME_GETTER, TableName);
         tableTypeCond = makeCondition(TableType);
      }

      private static final Column TableCatalog = new Column("TABLE_CATALOG", Type.String, null, Column.RESTRICTION, Column.REQUIRED,
               "The name of the catalog to which this object belongs.");
      private static final Column TableSchema = new Column("TABLE_SCHEMA", Type.String, null, Column.RESTRICTION, Column.OPTIONAL,
               "The name of the cube to which this object belongs.");
      private static final Column TableName = new Column("TABLE_NAME", Type.String, null, Column.RESTRICTION, Column.REQUIRED, "The name of the object, if TABLE_TYPE is TABLE.");
      private static final Column TableType = new Column("TABLE_TYPE", Type.String, null, Column.RESTRICTION, Column.REQUIRED,
               "The type of the table. TABLE indicates the object is a " + "measure group. SYSTEM TABLE indicates the object is a " + "dimension.");

      private static final Column TableGuid = new Column("TABLE_GUID", Type.UUID, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "Not supported.");
      private static final Column Description = new Column("DESCRIPTION", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "A human-readable description of the object.");
      private static final Column TablePropId = new Column("TABLE_PROPID", Type.UnsignedInteger, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "Not supported.");
      private static final Column DateCreated = new Column("DATE_CREATED", Type.DateTime, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "Not supported.");
      private static final Column DateModified = new Column("DATE_MODIFIED", Type.DateTime, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "The date the object was last modified.");

      private static final Column TableOlapType = new Column("TABLE_OLAP_TYPE", Type.String, null, Column.RESTRICTION, Column.REQUIRED,
               "The OLAP type of the object.  MEASURE_GROUP indicates the " + "object is a measure group.  CUBE_DIMENSION indicated the " + "object is a dimension.");

      private final static Long restrictionsMask = 31L;
      private final static String schemaCuid = "c8b52229-5cf3-11ce-ade5-00aa0044773d";

      public void populateImpl(XmlaResponse response, OlapConnection connection, List<Row> rows) throws XmlaException, OlapException {
         for (Catalog catalog : catIter(connection, catNameCond(), tableCatalogCond)) {
            // By definition, mondrian catalogs have only one
            // schema. It is safe to use get(0)
            final Schema schema = catalog.getSchemas().get(0);
            Row row;
            for (Cube cube : filter(sortedCubes(schema), tableNameCond)) {

               String desc = cube.getDescription();
               if (desc == null) {
                  desc = catalog.getName() + " - " + cube.getName() + " Cube";
               }

               if (tableTypeCond.apply("TABLE")) {
                  row = new Row();
                  row.set(TableCatalog.name, catalog.getName());
                  row.set(TableName.name, cube.getName());
                  row.set(TableSchema.name, cube.getName());
                  row.set(TableType.name, "TABLE");
                  row.set(Description.name, desc);
                  row.set(TableOlapType.name, "MEASURE_GROUP");
                  row.set(DateModified.name, dateModified);

                  addRow(row, rows);
               }

               if (tableTypeCond.apply("SYSTEM TABLE")) {
                  for (Dimension dimension : cube.getDimensions()) {
                     if (dimension.getDimensionType() == Dimension.Type.MEASURE) {
                        continue;
                     }
                     for (Hierarchy hierarchy : dimension.getHierarchies()) {
                        populateHierarchy(cube, hierarchy, rows);
                     }
                  }
               }
            }
         }
      }

      private void populateHierarchy(Cube cube, Hierarchy hierarchy, List<Row> rows) {
         for (Level level : hierarchy.getLevels()) {
            populateLevel(cube, hierarchy, level, rows);
         }
      }

      private void populateLevel(Cube cube, Hierarchy hierarchy, Level level, List<Row> rows) {
         String schemaName = cube.getSchema().getName();
         String cubeName = cube.getName();
         String hierarchyName = getHierarchyName(hierarchy);
         String levelName = level.getName();

         String tableName = cubeName + ':' + hierarchyName + ':' + levelName;

         String desc = level.getDescription();
         if (desc == null) {
            desc = schemaName + " - " + cubeName + " Cube - " + hierarchyName + " Hierarchy - " + levelName + " Level";
         }

         Row row = new Row();
         row.set(TableCatalog.name, schemaName);
         row.set(TableSchema.name, cubeName);
         row.set(TableName.name, tableName);
         row.set(TableType.name, "SYSTEM TABLE");
         row.set(Description.name, desc);
         row.set(TableOlapType.name, "CUBE_DIMENSION");
         row.set(DateModified.name, dateModified);
         addRow(row, rows);
      }

      protected void setProperty(PropertyDefinition propertyDef, String value) {
         switch (propertyDef) {
         case Content:
            break;
         default:
            super.setProperty(propertyDef, value);
         }
      }

      public static Long getRestrictionsmask() {
         return restrictionsMask;
      }

      public static String getSchemacuid() {
         return schemaCuid;
      }
   }

   static class MdschemaActionsRowset extends Rowset {
      MdschemaActionsRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(MDSCHEMA_ACTIONS, request, handler);
      }

      private static final Column CatalogName = new Column("CATALOG_NAME", Type.String, null, Column.RESTRICTION, Column.OPTIONAL,
               "The name of the catalog to which this action belongs.");
      private static final Column SchemaName = new Column("SCHEMA_NAME", Type.String, null, Column.RESTRICTION, Column.OPTIONAL,
               "The name of the schema to which this action belongs.");
      private static final Column CubeName = new Column("CUBE_NAME", Type.String, null, Column.RESTRICTION, Column.REQUIRED, "The name of the cube to which this action belongs.");
      private static final Column CubeSource = new Column("CUBE_SOURCE", Type.UnsignedShort, null, Column.RESTRICTION, Column.OPTIONAL, "");

      private static final Column ActionName = new Column("ACTION_NAME", Type.String, null, Column.RESTRICTION, Column.REQUIRED, "The name of the action.");

      private static final Column ActionType = new Column("ACTION_TYPE", Type.Integer, null, Column.RESTRICTION, Column.REQUIRED, "The name of the action.");

      private static final Column Invocation = new Column("INVOCATION", Type.Integer, null, Column.RESTRICTION, Column.REQUIRED,
               "Information about how to invoke the action: 1 - Indicates a regular action used during normal");

      private static final Column Coordinate = new Column("COORDINATE", Type.String, null, Column.RESTRICTION, Column.REQUIRED, null);
      private static final Column CoordinateType = new Column("COORDINATE_TYPE", Type.Integer, null, Column.RESTRICTION, Column.REQUIRED, null);

      public void populateImpl(XmlaResponse response, OlapConnection connection, List<Row> rows) throws XmlaException {
         // mondrian doesn't support actions. It's not an error to ask for
         // them, there just aren't any
      }
   }

   public static class MdschemaCubesRowset extends Rowset {
      private final Util.Functor1<Boolean, Catalog> catalogNameCond;
      private final Util.Functor1<Boolean, Schema> schemaNameCond;
      private final Util.Functor1<Boolean, Cube> cubeNameCond;

      MdschemaCubesRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(MDSCHEMA_CUBES, request, handler);
         catalogNameCond = makeCondition(CATALOG_NAME_GETTER, CatalogName);
         schemaNameCond = makeCondition(SCHEMA_NAME_GETTER, SchemaName);
         cubeNameCond = makeCondition(ELEMENT_NAME_GETTER, CubeName);
      }

      public static final String MD_CUBTYPE_CUBE = "CUBE";
      public static final String MD_CUBTYPE_VIRTUAL_CUBE = "VIRTUAL CUBE";

      private static final Column CatalogName = new Column("CATALOG_NAME", Type.String, null, Column.RESTRICTION, Column.OPTIONAL,
               "The name of the catalog to which this cube belongs.");
      private static final Column SchemaName = new Column("SCHEMA_NAME", Type.String, null, Column.RESTRICTION, Column.OPTIONAL,
               "The name of the schema to which this cube belongs.");
      private static final Column CubeName = new Column("CUBE_NAME", Type.String, null, Column.RESTRICTION, Column.REQUIRED, "Name of the cube.");
      private static final Column CubeType = new Column("CUBE_TYPE", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "Cube type.");
      private static final Column CubeGuid = new Column("CUBE_GUID", Type.UUID, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "Cube type.");
      private static final Column CreatedOn = new Column("CREATED_ON", Type.DateTime, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "Date and time of cube creation.");
      private static final Column LastSchemaUpdate = new Column("LAST_SCHEMA_UPDATE", Type.DateTime, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "Date and time of last schema update.");
      private static final Column SchemaUpdatedBy = new Column("SCHEMA_UPDATED_BY", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "User ID of the person who last updated the schema.");
      private static final Column LastDataUpdate = new Column("LAST_DATA_UPDATE", Type.DateTime, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "Date and time of last data update.");
      private static final Column DataUpdatedBy = new Column("DATA_UPDATED_BY", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "User ID of the person who last updated the data.");
      private static final Column IsDrillthroughEnabled = new Column("IS_DRILLTHROUGH_ENABLED", Type.Boolean, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "Describes whether DRILLTHROUGH can be performed on the " + "members of a cube");
      private static final Column IsWriteEnabled = new Column("IS_WRITE_ENABLED", Type.Boolean, null, Column.NOT_RESTRICTION, Column.REQUIRED,
               "Describes whether a cube is write-enabled");
      private static final Column IsLinkable = new Column("IS_LINKABLE", Type.Boolean, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "Describes whether a cube can be used in a linked cube");
      private static final Column IsSqlEnabled = new Column("IS_SQL_ENABLED", Type.Boolean, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "Describes whether or not SQL can be used on the cube");
      private static final Column CubeCaption = new Column("CUBE_CAPTION", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "The caption of the cube.");

      private static final Column BaseCubeName = new Column("BASE_CUBE_NAME", Type.String, null, Column.RESTRICTION, Column.REQUIRED,
               "The name of the source cube if this cube is a perspective cube");

      private static final Column CubeSource = new Column("CUBE_SOURCE", Type.UnsignedShort, null, Column.RESTRICTION, Column.REQUIRED,
               "A bitmask with one of these valid values: 0x01-Cube 0x02-Dimension");
      private static final Column PreferedQueryPatterns = new Column("PREFERRED_QUERY_PATTERNS", Type.UnsignedShort, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "describes query pattern client applications can utilize for higher performance");

      private static final Column Description = new Column("DESCRIPTION", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "A user-friendly description of the dimension.");
      private static final Column Dimensions = new Column("DIMENSIONS", Type.Rowset, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "Dimensions in this cube.");
      private static final Column Sets = new Column("SETS", Type.Rowset, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "Sets in this cube.");
      private static final Column Measures = new Column("MEASURES", Type.Rowset, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "Measures in this cube.");

      public void populateImpl(XmlaResponse response, OlapConnection connection, List<Row> rows) throws XmlaException, SQLException {
         for (Catalog catalog : catIter(connection, catNameCond(), catalogNameCond)) {
            for (Schema schema : filter(catalog.getSchemas(), schemaNameCond)) {
               for (Cube cube : filter(sortedCubes(schema), cubeNameCond)) {
                  String desc = cube.getDescription();
                  if (desc == null) {
                     desc = catalog.getName() + " Schema - " + cube.getName() + " Cube";
                  }

                  Row row = new Row();
                  row.set(CatalogName.name, catalog.getName());
                  row.set(CubeName.name, cube.getName());
                  final CustomXmlaHandler.XmlaExtra extra = getExtra(connection);
                  row.set(CubeType.name, extra.getCubeType(cube));
                  row.set(IsDrillthroughEnabled.name, true);
                  row.set(IsWriteEnabled.name, false);
                  row.set(IsLinkable.name, true);
                  row.set(IsSqlEnabled.name, true);
                  row.set(CubeCaption.name, cube.getCaption());
                  row.set(Description.name, desc);
                  Format formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
                  String formattedDate = formatter.format(extra.getSchemaLoadDate(schema));
                  row.set(LastSchemaUpdate.name, formattedDate);
                  row.set(LastDataUpdate.name, formattedDate);
                  if (deep) {
                     row.set(Dimensions.name,
                              new MdschemaDimensionsRowset(wrapRequest(request, Olap4jUtil.mapOf(MdschemaDimensionsRowset.CatalogName, catalog.getName(),
                                       MdschemaDimensionsRowset.SchemaName, schema.getName(), MdschemaDimensionsRowset.CubeName, cube.getName())), handler));
                     row.set(Sets.name,
                              new MdschemaSetsRowset(wrapRequest(request, Olap4jUtil.mapOf(MdschemaSetsRowset.CatalogName, catalog.getName(), MdschemaSetsRowset.SchemaName,
                                       schema.getName(), MdschemaSetsRowset.CubeName, cube.getName())), handler));
                     row.set(Measures.name,
                              new MdschemaMeasuresRowset(wrapRequest(request, Olap4jUtil.mapOf(MdschemaMeasuresRowset.CatalogName, catalog.getName(),
                                       MdschemaMeasuresRowset.SchemaName, schema.getName(), MdschemaMeasuresRowset.CubeName, cube.getName())), handler));
                  }
                  row.set(CubeSource.name, "1");
                  // row.set(BaseCubeName.name, cube.getName());
                  row.set(PreferedQueryPatterns.name, 0);
                  addRow(row, rows);
               }
            }
         }
      }

      protected void setProperty(PropertyDefinition propertyDef, String value) {
         switch (propertyDef) {
         case Content:
            break;
         default:
            super.setProperty(propertyDef, value);
         }
      }
   }

   static class MdschemaDimensionsRowset extends Rowset {
      private final Util.Functor1<Boolean, Catalog> catalogNameCond;
      private final Util.Functor1<Boolean, Schema> schemaNameCond;
      private final Util.Functor1<Boolean, Cube> cubeNameCond;
      private final Util.Functor1<Boolean, Dimension> dimensionUnameCond;
      private final Util.Functor1<Boolean, Dimension> dimensionNameCond;

      MdschemaDimensionsRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(MDSCHEMA_DIMENSIONS, request, handler);
         catalogNameCond = makeCondition(CATALOG_NAME_GETTER, CatalogName);
         schemaNameCond = makeCondition(SCHEMA_NAME_GETTER, SchemaName);
         cubeNameCond = makeCondition(ELEMENT_NAME_GETTER, CubeName);
         dimensionUnameCond = makeCondition(ELEMENT_UNAME_GETTER, DimensionUniqueName);
         dimensionNameCond = makeCondition(ELEMENT_NAME_GETTER, DimensionName);
      }

      public static final int MD_DIMTYPE_OTHER = 3;
      public static final int MD_DIMTYPE_MEASURE = 2;
      public static final int MD_DIMTYPE_TIME = 1;

      private static final Column CatalogName = new Column("CATALOG_NAME", Type.String, null, Column.RESTRICTION, Column.OPTIONAL, "The name of the database.");
      private static final Column SchemaName = new Column("SCHEMA_NAME", Type.String, null, Column.RESTRICTION, Column.OPTIONAL, "Not supported.");
      private static final Column CubeName = new Column("CUBE_NAME", Type.String, null, Column.RESTRICTION, Column.REQUIRED, "The name of the cube.");
      private static final Column DimensionName = new Column("DIMENSION_NAME", Type.String, null, Column.RESTRICTION, Column.REQUIRED, "The name of the dimension.");
      private static final Column DimensionUniqueName = new Column("DIMENSION_UNIQUE_NAME", Type.String, null, Column.RESTRICTION, Column.REQUIRED,
               "The unique name of the dimension.");
      private static final Column DimensionGuid = new Column("DIMENSION_GUID", Type.UUID, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "Not supported.");
      private static final Column DimensionCaption = new Column("DIMENSION_CAPTION", Type.String, null, Column.NOT_RESTRICTION, Column.REQUIRED, "The caption of the dimension.");
      private static final Column DimensionOrdinal = new Column("DIMENSION_ORDINAL", Type.UnsignedInteger, null, Column.NOT_RESTRICTION, Column.REQUIRED,
               "The position of the dimension within the cube.");
      /*
       * SQL Server returns values: MD_DIMTYPE_TIME (1) MD_DIMTYPE_MEASURE (2)
       * MD_DIMTYPE_OTHER (3)
       */
      private static final Column DimensionType = new Column("DIMENSION_TYPE", Type.Short, null, Column.NOT_RESTRICTION, Column.REQUIRED, "The type of the dimension.");
      private static final Column DimensionCardinality = new Column("DIMENSION_CARDINALITY", Type.UnsignedInteger, null, Column.NOT_RESTRICTION, Column.REQUIRED,
               "The number of members in the key attribute.");
      private static final Column DefaultHierarchy = new Column("DEFAULT_HIERARCHY", Type.String, null, Column.NOT_RESTRICTION, Column.REQUIRED,
               "A hierarchy from the dimension. Preserved for backwards " + "compatibility.");
      private static final Column Description = new Column("DESCRIPTION", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "A user-friendly description of the dimension.");
      private static final Column IsVirtual = new Column("IS_VIRTUAL", Type.Boolean, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "Always FALSE.");
      private static final Column IsReadWrite = new Column("IS_READWRITE", Type.Boolean, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "A Boolean that indicates whether the dimension is " + "write-enabled.");
      /*
       * SQL Server returns values: 0 or 1
       */
      private static final Column DimensionUniqueSettings = new Column("DIMENSION_UNIQUE_SETTINGS", Type.Integer, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "A bitmap that specifies which columns contain unique values " + "if the dimension contains only members with unique names.");
      private static final Column DimensionMasterUniqueName = new Column("DIMENSION_MASTER_NAME", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "Always NULL.");
      private static final Column DimensionVisibility = new Column("DIMENSION_VISIBILITY", Type.UnsignedShort, null, Column.RESTRICTION, Column.REQUIRED, "Always TRUE.");
      private static final Column Hierarchies = new Column("HIERARCHIES", Type.Rowset, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "Hierarchies in this dimension.");

      private static final Column CubeSource = new Column("CUBE_SOURCE", Type.UnsignedShort, null, Column.RESTRICTION, Column.OPTIONAL,
               "A bitmask with one of these valid values:0x01 - Cube 0x02 - Dimension");

      public void populateImpl(XmlaResponse response, OlapConnection connection, List<Row> rows) throws XmlaException, SQLException {
         for (Catalog catalog : catIter(connection, catNameCond(), catalogNameCond)) {
            populateCatalog(connection, catalog, rows);
         }
      }

      protected void populateCatalog(OlapConnection connection, Catalog catalog, List<Row> rows) throws XmlaException, SQLException {
         for (Schema schema : filter(catalog.getSchemas(), schemaNameCond)) {
            for (Cube cube : filteredCubes(schema, cubeNameCond)) {
               populateCube(connection, catalog, cube, rows);
            }
         }
      }

      protected void populateCube(OlapConnection connection, Catalog catalog, Cube cube, List<Row> rows) throws XmlaException, SQLException {
         for (Dimension dimension : filter(cube.getDimensions(), dimensionNameCond, dimensionUnameCond)) {
            populateDimension(connection, catalog, cube, dimension, rows);
         }
      }

      protected void populateDimension(OlapConnection connection, Catalog catalog, Cube cube, Dimension dimension, List<Row> rows) throws XmlaException, SQLException {
         String desc = dimension.getDescription();
         if (desc == null) {
            desc = cube.getName() + " Cube - " + dimension.getName() + " Dimension";
         }

         Row row = new Row();
         row.set(CatalogName.name, catalog.getName());
         row.set(SchemaName.name, cube.getSchema().getName());
         row.set(CubeName.name, cube.getName());
         row.set(DimensionName.name, dimension.getName());
         // row.set(DimensionMasterUniqueName.name, dimension.getName());

         row.set(DimensionUniqueName.name, dimension.getUniqueName());
         row.set(DimensionCaption.name, dimension.getCaption());
         row.set(DimensionOrdinal.name, cube.getDimensions().indexOf(dimension));
         row.set(DimensionType.name, getDimensionType(dimension));

         // Is this the number of primaryKey members there are??
         // According to microsoft this is:
         // "The number of members in the key attribute."
         // There may be a better way of doing this but
         // this is what I came up with. Note that I need to
         // add '1' to the number inorder for it to match
         // match what microsoft SQL Server is producing.
         // The '1' might have to do with whether or not the
         // hierarchy has a 'all' member or not - don't know yet.
         // large data set total for Orders cube 0m42.923s
         Hierarchy firstHierarchy = dimension.getHierarchies().get(0);
         NamedList<Level> levels = firstHierarchy.getLevels();
         Level lastLevel = levels.get(levels.size() - 1);

         /*
          * if override config setting is set if approxRowCount has a value use
          * it else do default
          */

         // Added by TWI to returned cached row numbers

         int n = getExtra(connection).getLevelCardinality(lastLevel);
         row.set(DimensionCardinality.name, n + 1);

         row.set(DefaultHierarchy.name, dimension.getUniqueName());
         row.set(Description.name, desc);
         row.set(IsVirtual.name, false);
         // SQL Server always returns false
         row.set(IsReadWrite.name, false);

         row.set(DimensionUniqueSettings.name, 0);
         row.set(DimensionMasterUniqueName.name, dimension.getName());
         row.set(DimensionVisibility.name, dimension.isVisible() ? 1 : 0);

         if (deep) {
            row.set(Hierarchies.name,
                     new MdschemaHierarchiesRowset(wrapRequest(request, Olap4jUtil.mapOf(MdschemaHierarchiesRowset.CatalogName, catalog.getName(),
                              MdschemaHierarchiesRowset.SchemaName, cube.getSchema().getName(), MdschemaHierarchiesRowset.CubeName, cube.getName(),
                              MdschemaHierarchiesRowset.DimensionUniqueName, dimension.getUniqueName())), handler));
         }

         addRow(row, rows);
      }

      protected void setProperty(PropertyDefinition propertyDef, String value) {
         switch (propertyDef) {
         case Content:
            break;
         default:
            super.setProperty(propertyDef, value);
         }
      }
   }

   static int getDimensionType(Dimension dim) throws OlapException {
      switch (dim.getDimensionType()) {
      case MEASURE:
         return MdschemaDimensionsRowset.MD_DIMTYPE_MEASURE;
      case TIME:
         return MdschemaDimensionsRowset.MD_DIMTYPE_TIME;
      default:
         return MdschemaDimensionsRowset.MD_DIMTYPE_OTHER;
      }
   }

   public static class MdschemaFunctionsRowset extends Rowset {
      /**
       * http://www.csidata.com/custserv/onlinehelp/VBSdocs/vbs57.htm
       */
      public enum VarType {
         Empty("Uninitialized (default)"), Null("Contains no valid data"), Integer("Integer subtype"), Long("Long subtype"), Single("Single subtype"), Double("Double subtype"), Currency(
                  "Currency subtype"), Date("Date subtype"), String("String subtype"), Object("Object subtype"), Error("Error subtype"), Boolean("Boolean subtype"), Variant(
                  "Variant subtype"), DataObject("DataObject subtype"), Decimal("Decimal subtype"), Byte("Byte subtype"), Array("Array subtype");

         public static VarType forCategory(int category) {
            switch (category) {
            case Category.Unknown:
               // expression == unknown ???
               // case Category.Expression:
               return Empty;
            case Category.Array:
               return Array;
            case Category.Dimension:
            case Category.Hierarchy:
            case Category.Level:
            case Category.Member:
            case Category.Set:
            case Category.Tuple:
            case Category.Cube:
            case Category.Value:
               return Variant;
            case Category.Logical:
               return Boolean;
            case Category.Numeric:
               return Double;
            case Category.String:
            case Category.Symbol:
            case Category.Constant:
               return String;
            case Category.DateTime:
               return Date;
            case Category.Integer:
            case Category.Mask:
               return Integer;
            }
            // NOTE: this should never happen
            return Empty;
         }

         VarType(String description) {
            Util.discard(description);
         }
      }

      private final Util.Functor1<Boolean, String> functionNameCond;

      MdschemaFunctionsRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(MDSCHEMA_FUNCTIONS, request, handler);
         functionNameCond = makeCondition(FunctionName);
      }

      private static final Column FunctionName = new Column("FUNCTION_NAME", Type.String, null, Column.RESTRICTION, Column.REQUIRED, "The name of the function.");
      private static final Column Description = new Column("DESCRIPTION", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "A description of the function.");
      private static final Column ParameterList = new Column("PARAMETER_LIST", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "A comma delimited list of parameters.");
      private static final Column ReturnType = new Column("RETURN_TYPE", Type.Integer, null, Column.NOT_RESTRICTION, Column.REQUIRED,
               "The VARTYPE of the return data type of the function.");
      private static final Column Origin = new Column("ORIGIN", Type.Integer, null, Column.RESTRICTION, Column.REQUIRED,
               "The origin of the function:  1 for MDX functions.  2 for " + "user-defined functions.");
      private static final Column InterfaceName = new Column("INTERFACE_NAME", Type.String, null, Column.RESTRICTION, Column.REQUIRED,
               "The name of the interface for user-defined functions");
      private static final Column LibraryName = new Column("LIBRARY_NAME", Type.String, null, Column.RESTRICTION, Column.OPTIONAL,
               "The name of the type library for user-defined functions. " + "NULL for MDX functions.");
      private static final Column Caption = new Column("CAPTION", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "The display caption for the function.");

      public void populateImpl(XmlaResponse response, OlapConnection connection, List<Row> rows) throws XmlaException, SQLException {
         final CustomXmlaHandler.XmlaExtra extra = getExtra(connection);
         for (Catalog catalog : catIter(connection, catNameCond())) {
            // By definition, mondrian catalogs have only one
            // schema. It is safe to use get(0)
            final Schema schema = catalog.getSchemas().get(0);
            List<CustomXmlaHandler.XmlaExtra.FunctionDefinition> funDefs = new ArrayList<CustomXmlaHandler.XmlaExtra.FunctionDefinition>();

            // olap4j does not support describing functions. Call an
            // auxiliary method.
            extra.getSchemaFunctionList(funDefs, schema, functionNameCond);
            for (CustomXmlaHandler.XmlaExtra.FunctionDefinition funDef : funDefs) {
               Row row = new Row();
               row.set(FunctionName.name, funDef.functionName);
               row.set(Description.name, funDef.description);
               row.set(ParameterList.name, funDef.parameterList);
               row.set(ReturnType.name, funDef.returnType);
               row.set(Origin.name, funDef.origin);
               // row.set(LibraryName.name, "");
               row.set(InterfaceName.name, funDef.interfaceName);
               row.set(Caption.name, funDef.caption);
               addRow(row, rows);
            }
         }
      }

      protected void setProperty(PropertyDefinition propertyDef, String value) {
         switch (propertyDef) {
         case Content:
            break;
         default:
            super.setProperty(propertyDef, value);
         }
      }
   }

   static class MdschemaHierarchiesRowset extends Rowset {
      private final Util.Functor1<Boolean, Catalog> catalogCond;
      private final Util.Functor1<Boolean, Schema> schemaNameCond;
      private final Util.Functor1<Boolean, Cube> cubeNameCond;
      private final Util.Functor1<Boolean, Dimension> dimensionUnameCond;
      private final Util.Functor1<Boolean, Hierarchy> hierarchyUnameCond;
      private final Util.Functor1<Boolean, Hierarchy> hierarchyNameCond;

      MdschemaHierarchiesRowset(XmlaRequest request, CustomXmlaHandler handler) {

         super(MDSCHEMA_HIERARCHIES, request, handler);
         // if (((DefaultXmlaRequest)request).getRequestItemName() != null &&
         // ((DefaultXmlaRequest)request).getRequestItemName().equals("MDSCHEMA_HIERARCHIES")){
         // if(getRestriction("CUBE_NAME")== null){
         // putRestriction("CUBE_NAME", Arrays.asList(handler.currentCube));
         // }
         // }

         catalogCond = makeCondition(CATALOG_NAME_GETTER, CatalogName);
         schemaNameCond = makeCondition(SCHEMA_NAME_GETTER, SchemaName);
         cubeNameCond = makeCondition(ELEMENT_NAME_GETTER, CubeName);
         dimensionUnameCond = makeCondition(ELEMENT_UNAME_GETTER, DimensionUniqueName);
         hierarchyUnameCond = makeCondition(ELEMENT_UNAME_GETTER, HierarchyUniqueName);
         hierarchyNameCond = makeCondition(ELEMENT_NAME_GETTER, HierarchyName);
      }

      private static final Column CatalogName = new Column("CATALOG_NAME", Type.String, null, Column.RESTRICTION, Column.OPTIONAL,
               "The name of the catalog to which this hierarchy belongs.");
      private static final Column SchemaName = new Column("SCHEMA_NAME", Type.String, null, Column.RESTRICTION, Column.OPTIONAL, "Not supported");
      private static final Column CubeName = new Column("CUBE_NAME", Type.String, null, Column.RESTRICTION, Column.OPTIONAL,
               "The name of the cube to which this hierarchy belongs.");

      private static final Column CubeSource = new Column("CUBE_SOURCE", Type.UnsignedShort, null, Column.RESTRICTION, Column.OPTIONAL, "");
      private static final Column DimensionUniqueName = new Column("DIMENSION_UNIQUE_NAME", Type.String, null, Column.OPTIONAL, Column.OPTIONAL,
               "The unique name of the dimension to which this hierarchy " + "belongs.");

      private static final Column DimensionMasterUniqueName = new Column("DIMENSION_MASTER_UNIQUE_NAME", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "The unique name of the dimension to which this hierarchy " + "belongs.");
      private static final Column HierarchyName = new Column("HIERARCHY_NAME", Type.String, null, Column.RESTRICTION, Column.OPTIONAL,
               "The name of the hierarchy. Blank if there is only a single " + "hierarchy in the dimension.");
      private static final Column HierarchyUniqueName = new Column("HIERARCHY_UNIQUE_NAME", Type.String, null, Column.OPTIONAL, Column.OPTIONAL,
               "The unique name of the hierarchy.");

      private static final Column HierarchyGuid = new Column("HIERARCHY_GUID", Type.UUID, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "Hierarchy GUID.");

      private static final Column HierarchyCaption = new Column("HIERARCHY_CAPTION", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "A label or a caption associated with the hierarchy.");
      private static final Column DimensionType = new Column("DIMENSION_TYPE", Type.Short, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "The type of the dimension.");
      private static final Column HierarchyCardinality = new Column("HIERARCHY_CARDINALITY", Type.UnsignedInteger, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "The number of members in the hierarchy.");
      private static final Column DefaultMember = new Column("DEFAULT_MEMBER", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "The default member for this hierarchy.");
      private static final Column AllMember = new Column("ALL_MEMBER", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "The member at the highest level of rollup in the hierarchy.");
      private static final Column Description = new Column("DESCRIPTION", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "A human-readable description of the hierarchy. NULL if no " + "description exists.");
      private static final Column Structure = new Column("STRUCTURE", Type.Short, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "The structure of the hierarchy.");
      private static final Column IsVirtual = new Column("IS_VIRTUAL", Type.Boolean, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "Always returns False.");
      private static final Column IsReadWrite = new Column("IS_READWRITE", Type.Boolean, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "A Boolean that indicates whether the Write Back to dimension " + "column is enabled.");
      private static final Column HierarchyUniqueSettings = new Column("HIERARCHY_UNIQUE_SETTINGS", Type.Integer, null, Column.NOT_RESTRICTION, Column.REQUIRED,
               "Always returns MDDIMENSIONS_MEMBER_KEY_UNIQUE (1).");
      private static final Column DimensionIsVisible = new Column("DIMENSION_IS_VISIBLE", Type.Boolean, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "A Boolean that indicates whether the parent dimension is visible.");
      private static final Column HierarchyIsVisible = new Column("HIERARCHY_IS_VISIBLE", Type.Boolean, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "A Boolean that indicates whether the hieararchy is visible.");
      private static final Column HierarchyVisibility = new Column("HIERARCHY_VISIBILITY", Type.UnsignedShort, null, Column.RESTRICTION, Column.OPTIONAL, "");
      private static final Column HierarchyOrdinal = new Column("HIERARCHY_ORDINAL", Type.UnsignedInteger, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "The ordinal number of the hierarchy across all hierarchies of " + "the cube.");
      private static final Column DimensionIsShared = new Column("DIMENSION_IS_SHARED", Type.Boolean, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "Always returns true.");
      private static final Column Levels = new Column("LEVELS", Type.Rowset, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "Levels in this hierarchy.");
      private static final Column DimensionUniqueSettings = new Column("DIMENSION_UNIQUE_SETTINGS", Type.Integer, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");

      private static final Column HierarchyOrigin = new Column("HIERARCHY_ORIGIN", Type.UnsignedShort, null, Column.RESTRICTION, Column.OPTIONAL, "Levels in this hierarchy.");
      private static final Column HierarchyDisplayFolder = new Column("HIERARCHY_DISPLAY_FOLDER", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");

      /*
       * NOTE: This is non-standard, where did it come from?
       */
      // private static final Column ParentChild = new Column("PARENT_CHILD",
      // Type.Boolean, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
      // "Is hierarchy a parent.");

      private static final Column InstanceSelection = new Column("INSTANCE_SELECTION", Type.UnsignedShort, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");

      private static final Column StructureType = new Column("STRUCTURE_TYPE", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");

      private static final Column GroupingBehaviors = new Column("GROUPING_BEHAVIOR", Type.UnsignedShort, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");

      public void populateImpl(XmlaResponse response, OlapConnection connection, List<Row> rows) throws XmlaException, SQLException {

         // if(visualModeEnable)
         // return;

         for (Catalog catalog : catIter(connection, catNameCond(), catalogCond)) {
            populateCatalog(connection, catalog, rows);
         }
      }

      protected void populateCatalog(OlapConnection connection, Catalog catalog, List<Row> rows) throws XmlaException, SQLException {
         for (Schema schema : filter(catalog.getSchemas(), schemaNameCond)) {
            for (Cube cube : filteredCubes(schema, cubeNameCond)) {
               if (cube instanceof SharedDimensionHolderCube) {
                  continue;
               }
               populateCube(connection, catalog, cube, rows);

            }
         }
      }

      protected void populateCube(OlapConnection connection, Catalog catalog, Cube cube, List<Row> rows) throws XmlaException, SQLException {
         int ordinal = 0;
         for (Dimension dimension : cube.getDimensions()) {
            // Must increment ordinal for all dimensions but
            // only output some of them.
            boolean genOutput = dimensionUnameCond.apply(dimension);
            if (genOutput) {
               populateDimension(connection, catalog, cube, dimension, ordinal, rows);
            }
            ordinal += dimension.getHierarchies().size();
         }
      }

      protected void populateDimension(OlapConnection connection, Catalog catalog, Cube cube, Dimension dimension, int ordinal, List<Row> rows) throws XmlaException, SQLException {
         final NamedList<Hierarchy> hierarchies = dimension.getHierarchies();
         for (Hierarchy hierarchy : filter(hierarchies, hierarchyNameCond, hierarchyUnameCond)) {

            populateHierarchy(connection, catalog, cube, dimension, hierarchy, ordinal + hierarchies.indexOf(hierarchy), rows);

         }
      }

      protected void populateHierarchy(OlapConnection connection, Catalog catalog, Cube cube, Dimension dimension, Hierarchy hierarchy,
      // Level level,
               int ordinal, List<Row> rows) throws XmlaException, SQLException {
         final CustomXmlaHandler.XmlaExtra extra = getExtra(connection);
         String desc = hierarchy.getDescription();
         if (desc == null) {
            desc = cube.getName() + " Cube - " + getHierarchyName(hierarchy) + " Hierarchy";
         }

         Row row = new Row();
         row.set(CatalogName.name, catalog.getName());
         // row.set(SchemaName.name, cube.getSchema().getName());
         row.set(CubeName.name, cube.getName());
         row.set(DimensionUniqueName.name, dimension.getUniqueName());
         row.set(HierarchyName.name, hierarchy.getName());
         row.set(HierarchyUniqueName.name, hierarchy.getUniqueName());

         // row.set(HierarchyGuid.name, "");

         row.set(HierarchyCaption.name, hierarchy.getCaption());
         row.set(DimensionType.name, getDimensionType(dimension));
         // The number of members in the hierarchy. Because
         // of the presence of multiple hierarchies, this number
         // might not be the same as DIMENSION_CARDINALITY. This
         // value can be an approximation of the real
         // cardinality. Consumers should not assume that this
         // value is accurate.
         int cardinality = extra.getHierarchyCardinality(hierarchy);
         row.set(HierarchyCardinality.name, cardinality);

         row.set(DefaultMember.name, hierarchy.getDefaultMember().getUniqueName());
         if (hierarchy.hasAll()) {
            row.set(AllMember.name, hierarchy.getRootMembers().get(0).getUniqueName());
         }
         row.set(Description.name, desc);

         // only support:
         // MD_STRUCTURE_FULLYBALANCED (0)
         // MD_STRUCTURE_RAGGEDBALANCED (1)
         row.set(Structure.name, extra.getHierarchyStructure(hierarchy));

         row.set(IsVirtual.name, false);
         row.set(IsReadWrite.name, false);

         // NOTE that SQL Server returns '0'
         row.set(HierarchyUniqueSettings.name, 0);
         row.set(DimensionUniqueSettings.name, 1);
         row.set(DimensionIsVisible.name, dimension.isVisible());
         row.set(HierarchyIsVisible.name, hierarchy.isVisible());

         row.set(HierarchyOrdinal.name, ordinal);

         // always true
         row.set(DimensionIsShared.name, true);

         // row.set(ParentChild.name, extra.isHierarchyParentChild(hierarchy));
         if (deep) {
            row.set(Levels.name,
                     new MdschemaLevelsRowset(wrapRequest(request, Olap4jUtil.mapOf(MdschemaLevelsRowset.CatalogName, catalog.getName(), MdschemaLevelsRowset.SchemaName, cube
                              .getSchema().getName(), MdschemaLevelsRowset.CubeName, cube.getName(), MdschemaLevelsRowset.DimensionUniqueName, dimension.getUniqueName(),
                              MdschemaLevelsRowset.HierarchyUniqueName, hierarchy.getUniqueName())), handler));
         }

         // row.set(InstanceSelection.name, 1);
         row.set(HierarchyDisplayFolder.name, "");
         row.set(GroupingBehaviors.name, 1);
         row.set(HierarchyOrigin.name, 1);
         row.set(StructureType.name, "Natural");

         addRow(row, rows);
      }

      protected void setProperty(PropertyDefinition propertyDef, String value) {
         switch (propertyDef) {
         case Content:
            break;
         default:
            super.setProperty(propertyDef, value);
         }
      }
   }

   static class DiscoverDatasourcesRowset extends Rowset {

      private static final Column DataSourceName = new Column("DataSourceName", Type.String, null, Column.RESTRICTION, Column.REQUIRED,
               "The name of the data source, such as FoodMart 2000.");
      private static final Column DataSourceDescription = new Column("DataSourceDescription", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "A description of the data source, as entered by the " + "publisher.");
      private static final Column URL = new Column("URL", Type.String, null, Column.RESTRICTION, Column.OPTIONAL, "The unique path that shows where to invoke the XML for "
               + "Analysis methods for that data source.");
      private static final Column DataSourceInfo = new Column("DataSourceInfo", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "A string containing any additional information required to " + "connect to the data source. This can include the Initial "
                        + "Catalog property or other information for the provider.\n" + "Example: \"Provider=MSOLAP;Data Source=Local;\"");
      private static final Column ProviderName = new Column("ProviderName", Type.String, null, Column.RESTRICTION, Column.OPTIONAL,
               "The name of the provider behind the data source.\n" + "Example: \"MSDASQL\"");
      private static final Column ProviderType = new Column("ProviderType", Type.EnumerationArray, Enumeration.PROVIDER_TYPE, Column.RESTRICTION, Column.REQUIRED,
               Column.UNBOUNDED, "The types of data supported by the provider. May include one " + "or more of the following types. Example follows this " + "table.\n"
                        + "TDP: tabular data provider.\n" + "MDP: multidimensional data provider.\n" + "DMP: data mining provider. A DMP provider implements the "
                        + "OLE DB for Data Mining specification.");
      private static final Column AuthenticationMode = new Column("AuthenticationMode", Type.EnumString, Enumeration.AUTHENTICATION_MODE, Column.RESTRICTION, Column.REQUIRED,
               "Specification of what type of security mode the data source " + "uses. Values can be one of the following:\n"
                        + "Unauthenticated: no user ID or password needs to be sent.\n" + "Authenticated: User ID and Password must be included in the "
                        + "information required for the connection.\n" + "Integrated: the data source uses the underlying security to "
                        + "determine authorization, such as Integrated Security " + "provided by Microsoft Internet Information Services (IIS).");

      DiscoverDatasourcesRowset(RowsetDefinition definition, XmlaRequest request, CustomXmlaHandler handler) {
         super(definition, request, handler);
      }

      public DiscoverDatasourcesRowset(XmlaRequest request, CustomXmlaHandler handler) {
         this(DISCOVER_DATASOURCES, request, handler);
      }

      private static final Column[] columns = { DataSourceName, DataSourceDescription, URL, DataSourceInfo, ProviderName, ProviderType, AuthenticationMode, };

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List<Row> rows) throws XmlaException, SQLException {
         if (needConnection()) {
            final CustomXmlaHandler.XmlaExtra extra = getExtra(connection);
            for (Map<String, Object> ds : extra.getDataSources(connection)) {
               Row row = new Row();
               for (Column column : columns) {
                  Object val = ds.get(column.name);
                  row.set(column.name, val);
               }
               addRow(row, rows);
            }
         } else {
            // using pre-configured discover datasources response
            Row row = new Row();
            Map<String, Object> map = this.handler.connectionFactory.getPreConfiguredDiscoverDatasourcesResponse();
            for (Column column : columns) {
               row.set(column.name, map.get(column.name));
            }
            addRow(row, rows);
         }
      }

      @Override
      protected boolean needConnection() {
         // If the olap connection factory has a pre configured response,
         // we don't need to connect to find metadata. This is good.
         return this.handler.connectionFactory.getPreConfiguredDiscoverDatasourcesResponse() == null;
      }

      protected void setProperty(PropertyDefinition propertyDef, String value) {
         switch (propertyDef) {
         case Content:
            break;
         default:
            super.setProperty(propertyDef, value);
         }
      }

   }

   static class MdschemaLevelsRowset extends Rowset {
      private final Util.Functor1<Boolean, Catalog> catalogCond;
      private final Util.Functor1<Boolean, Schema> schemaNameCond;
      private final Util.Functor1<Boolean, Cube> cubeNameCond;
      private final Util.Functor1<Boolean, Dimension> dimensionUnameCond;
      private final Util.Functor1<Boolean, Hierarchy> hierarchyUnameCond;
      private final Util.Functor1<Boolean, Level> levelUnameCond;
      private final Util.Functor1<Boolean, Level> levelNameCond;

      MdschemaLevelsRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(MDSCHEMA_LEVELS, request, handler);
         catalogCond = makeCondition(CATALOG_NAME_GETTER, CatalogName);
         schemaNameCond = makeCondition(SCHEMA_NAME_GETTER, SchemaName);
         cubeNameCond = makeCondition(ELEMENT_NAME_GETTER, CubeName);
         dimensionUnameCond = makeCondition(ELEMENT_UNAME_GETTER, DimensionUniqueName);
         hierarchyUnameCond = makeCondition(ELEMENT_UNAME_GETTER, HierarchyUniqueName);
         levelUnameCond = makeCondition(ELEMENT_UNAME_GETTER, LevelUniqueName);
         levelNameCond = makeCondition(ELEMENT_NAME_GETTER, LevelName);
      }

      public static final int MDLEVEL_TYPE_UNKNOWN = 0x0000;
      public static final int MDLEVEL_TYPE_REGULAR = 0x0000;
      public static final int MDLEVEL_TYPE_ALL = 0x0001;
      public static final int MDLEVEL_TYPE_CALCULATED = 0x0002;
      public static final int MDLEVEL_TYPE_TIME = 0x0004;
      public static final int MDLEVEL_TYPE_RESERVED1 = 0x0008;
      public static final int MDLEVEL_TYPE_TIME_YEARS = 0x0014;
      public static final int MDLEVEL_TYPE_TIME_HALF_YEAR = 0x0024;
      public static final int MDLEVEL_TYPE_TIME_QUARTERS = 0x0044;
      public static final int MDLEVEL_TYPE_TIME_MONTHS = 0x0084;
      public static final int MDLEVEL_TYPE_TIME_WEEKS = 0x0104;
      public static final int MDLEVEL_TYPE_TIME_DAYS = 0x0204;
      public static final int MDLEVEL_TYPE_TIME_HOURS = 0x0304;
      public static final int MDLEVEL_TYPE_TIME_MINUTES = 0x0404;
      public static final int MDLEVEL_TYPE_TIME_SECONDS = 0x0804;
      public static final int MDLEVEL_TYPE_TIME_UNDEFINED = 0x1004;

      private static final Column CatalogName = new Column("CATALOG_NAME", Type.String, null, Column.RESTRICTION, Column.OPTIONAL,
               "The name of the catalog to which this level belongs.");
      private static final Column SchemaName = new Column("SCHEMA_NAME", Type.String, null, Column.RESTRICTION, Column.OPTIONAL,
               "The name of the schema to which this level belongs.");
      private static final Column CubeName = new Column("CUBE_NAME", Type.String, null, Column.RESTRICTION, Column.REQUIRED, "The name of the cube to which this level belongs.");
      private static final Column CubeSource = new Column("CUBE_SOURCE", Type.UnsignedShort, null, Column.RESTRICTION, Column.OPTIONAL, "");

      private static final Column DimensionUniqueName = new Column("DIMENSION_UNIQUE_NAME", Type.String, null, Column.RESTRICTION, Column.REQUIRED,
               "The unique name of the dimension to which this level " + "belongs.");
      private static final Column HierarchyUniqueName = new Column("HIERARCHY_UNIQUE_NAME", Type.String, null, Column.RESTRICTION, Column.REQUIRED,
               "The unique name of the hierarchy.");
      private static final Column LevelName = new Column("LEVEL_NAME", Type.String, null, Column.RESTRICTION, Column.REQUIRED, "The name of the level.");
      private static final Column LevelUniqueName = new Column("LEVEL_UNIQUE_NAME", Type.String, null, Column.RESTRICTION, Column.REQUIRED,
               "The properly escaped unique name of the level.");
      private static final Column LevelGuid = new Column("LEVEL_GUID", Type.UUID, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "Level GUID.");
      private static final Column LevelCaption = new Column("LEVEL_CAPTION", Type.String, null, Column.NOT_RESTRICTION, Column.REQUIRED,
               "A label or caption associated with the hierarchy.");
      private static final Column LevelNumber = new Column("LEVEL_NUMBER", Type.UnsignedInteger, null, Column.NOT_RESTRICTION, Column.REQUIRED,
               "The distance of the level from the root of the hierarchy. " + "Root level is zero (0).");
      private static final Column LevelCardinality = new Column("LEVEL_CARDINALITY", Type.UnsignedInteger, null, Column.NOT_RESTRICTION, Column.REQUIRED,
               "The number of members in the level. This value can be an " + "approximation of the real cardinality.");
      private static final Column LevelType = new Column("LEVEL_TYPE", Type.Integer, null, Column.NOT_RESTRICTION, Column.REQUIRED, "Type of the level");
      private static final Column CustomRollupSettings = new Column("CUSTOM_ROLLUP_SETTINGS", Type.Integer, null, Column.NOT_RESTRICTION, Column.REQUIRED,
               "A bitmap that specifies the custom rollup options.");
      private static final Column LevelUniqueSettings = new Column("LEVEL_UNIQUE_SETTINGS", Type.Integer, null, Column.NOT_RESTRICTION, Column.REQUIRED,
               "A bitmap that specifies which columns contain unique values, " + "if the level only has members with unique names or keys.");
      private static final Column LevelVisibility = new Column("LEVEL_VISIBILITY", Type.UnsignedShort, null, Column.RESTRICTION, Column.REQUIRED,
               "A Boolean that indicates whether the level is visible.");
      private static final Column Description = new Column("DESCRIPTION", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "A human-readable description of the level. NULL if no " + "description exists.");
      private static final Column LevelKeyCardinality = new Column("LEVEL_KEY_CARDINALITY", Type.Short, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");
      private static final Column LevelOrigin = new Column("LEVEL_ORIGIN", Type.UnsignedShort, null, Column.RESTRICTION, Column.OPTIONAL, "");

      private static final Column LevelDbtype = new Column("LEVEL_DBTYPE", Type.Integer, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");
      private static final Column LevelOrderingProperty = new Column("LEVEL_ORDERING_PROPERTY", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");
      private static final Column LevelNameSqlColumnName = new Column("LEVEL_NAME_SQL_COLUMN_NAME", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");
      private static final Column LevelKeySqlColumnName = new Column("LEVEL_KEY_SQL_COLUMN_NAME", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");
      private static final Column LevelUniqueNameSqlColumnName = new Column("LEVEL_UNIQUE_NAME_SQL_COLUMN_NAME", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");
      private static final Column LevelAttributeHierarchyName = new Column("LEVEL_ATTRIBUTE_HIERARCHY_NAME", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");

      public void populateImpl(XmlaResponse response, OlapConnection connection, List<Row> rows) throws XmlaException, SQLException {
         for (Catalog catalog : catIter(connection, catNameCond(), catalogCond)) {
            populateCatalog(connection, catalog, rows);
         }
      }

      protected void populateCatalog(OlapConnection connection, Catalog catalog, List<Row> rows) throws XmlaException, SQLException {
         for (Schema schema : filter(catalog.getSchemas(), schemaNameCond)) {
            for (Cube cube : filteredCubes(schema, cubeNameCond)) {
               populateCube(connection, catalog, cube, rows);
            }
         }
      }

      protected void populateCube(OlapConnection connection, Catalog catalog, Cube cube, List<Row> rows) throws XmlaException, SQLException {
         for (Dimension dimension : filter(cube.getDimensions(), dimensionUnameCond)) {
            populateDimension(connection, catalog, cube, dimension, rows);
         }
      }

      protected void populateDimension(OlapConnection connection, Catalog catalog, Cube cube, Dimension dimension, List<Row> rows) throws XmlaException, SQLException {
         for (Hierarchy hierarchy : filter(dimension.getHierarchies(), hierarchyUnameCond)) {
            populateHierarchy(connection, catalog, cube, hierarchy, rows);
         }
      }

      protected void populateHierarchy(OlapConnection connection, Catalog catalog, Cube cube, Hierarchy hierarchy, List<Row> rows) throws XmlaException, SQLException {
         for (Level level : filter(hierarchy.getLevels(), levelUnameCond, levelNameCond)) {
            outputLevel(connection, catalog, cube, hierarchy, level, rows);
         }
      }

      /**
       * Outputs a level.
       * 
       * @param catalog
       *           Catalog name
       * @param cube
       *           Cube definition
       * @param hierarchy
       *           Hierarchy
       * @param level
       *           Level
       * @param rows
       *           List of rows to output to
       * @return whether the level is visible
       * @throws XmlaException
       *            If error occurs
       */
      protected boolean outputLevel(OlapConnection connection, Catalog catalog, Cube cube, Hierarchy hierarchy, Level level, List<Row> rows) throws XmlaException, SQLException {
         final CustomXmlaHandler.XmlaExtra extra = getExtra(connection);
         String desc = level.getDescription();
         if (desc == null) {
            desc = cube.getName() + " Cube - " + getHierarchyName(hierarchy) + " Hierarchy - " + level.getName() + " Level";
         }

         Row row = new Row();
         row.set(CatalogName.name, catalog.getName());
         row.set(SchemaName.name, cube.getSchema().getName());
         row.set(CubeName.name, cube.getName());
         row.set(DimensionUniqueName.name, hierarchy.getDimension().getUniqueName());
         row.set(HierarchyUniqueName.name, hierarchy.getUniqueName());
         row.set(LevelName.name, level.getName());
         row.set(LevelUniqueName.name, level.getUniqueName());
         row.set(LevelCaption.name, level.getCaption());
         row.set(LevelNumber.name, level.getDepth());

         // Get level cardinality
         // According to microsoft this is:
         // "The number of members in the level."
         int n = extra.getLevelCardinality(level);
         row.set(LevelCardinality.name, n);
         row.set(LevelType.name, getLevelType(level));
         row.set(CustomRollupSettings.name, 0);

         int uniqueSettings = 0;
         if (level.getLevelType() == Level.Type.ALL) {
            uniqueSettings |= 2;
         }
         if (extra.isLevelUnique(level)) {
            uniqueSettings |= 1;
         }
         row.set(LevelUniqueSettings.name, uniqueSettings);
         int levelVisibility = level.isVisible() ? 1 : 0;
         row.set(LevelVisibility.name, (short) levelVisibility);
         row.set(LevelOrderingProperty.name, level.getName());
         row.set(Description.name, desc);
         row.set(LevelDbtype.name, 3);
         row.set(LevelKeyCardinality.name, 1);
         row.set(LevelOrigin.name, 2);

         if (!level.getName().equalsIgnoreCase("(All)")) {
            String tmpName = level.getUniqueName();
            String str = tmpName.substring(1);
            tmpName = "[$".concat(str);

            row.set(LevelNameSqlColumnName.name, "NAME(" + tmpName + ")");
            row.set(LevelKeySqlColumnName.name, "KEY(" + tmpName + ")");
            row.set(LevelUniqueNameSqlColumnName.name, "(UNIQUENAME(" + tmpName + ")");
            row.set(LevelAttributeHierarchyName.name, level.getName());

         }
         addRow(row, rows);
         return true;
      }

      private int getLevelType(Level lev) {
         int ret = 0;

         switch (lev.getLevelType()) {
         case ALL:
            ret |= MDLEVEL_TYPE_ALL;
            break;
         case REGULAR:
            ret |= MDLEVEL_TYPE_REGULAR;
            break;
         case TIME_YEARS:
            ret |= MDLEVEL_TYPE_TIME_YEARS;
            break;
         case TIME_HALF_YEAR:
            ret |= MDLEVEL_TYPE_TIME_HALF_YEAR;
            break;
         case TIME_QUARTERS:
            ret |= MDLEVEL_TYPE_TIME_QUARTERS;
            break;
         case TIME_MONTHS:
            ret |= MDLEVEL_TYPE_TIME_MONTHS;
            break;
         case TIME_WEEKS:
            ret |= MDLEVEL_TYPE_TIME_WEEKS;
            break;
         case TIME_DAYS:
            ret |= MDLEVEL_TYPE_TIME_DAYS;
            break;
         case TIME_HOURS:
            ret |= MDLEVEL_TYPE_TIME_HOURS;
            break;
         case TIME_MINUTES:
            ret |= MDLEVEL_TYPE_TIME_MINUTES;
            break;
         case TIME_SECONDS:
            ret |= MDLEVEL_TYPE_TIME_SECONDS;
            break;
         case TIME_UNDEFINED:
            ret |= MDLEVEL_TYPE_TIME_UNDEFINED;
            break;
         default:
            ret |= MDLEVEL_TYPE_UNKNOWN;
         }

         return ret;
      }

      protected void setProperty(PropertyDefinition propertyDef, String value) {
         switch (propertyDef) {
         case Content:
            break;
         default:
            super.setProperty(propertyDef, value);
         }
      }
   }

   public static class MdschemaMeasuresRowset extends Rowset {
      public static final int MDMEASURE_AGGR_UNKNOWN = 0;
      public static final int MDMEASURE_AGGR_SUM = 1;
      public static final int MDMEASURE_AGGR_COUNT = 2;
      public static final int MDMEASURE_AGGR_MIN = 3;
      public static final int MDMEASURE_AGGR_MAX = 4;
      public static final int MDMEASURE_AGGR_AVG = 5;
      public static final int MDMEASURE_AGGR_VAR = 6;
      public static final int MDMEASURE_AGGR_STD = 7;
      public static final int MDMEASURE_AGGR_CALCULATED = 127;

      private final Util.Functor1<Boolean, Catalog> catalogCond;
      private final Util.Functor1<Boolean, Schema> schemaNameCond;
      private final Util.Functor1<Boolean, Cube> cubeNameCond;
      private final Util.Functor1<Boolean, Measure> measureUnameCond;
      private final Util.Functor1<Boolean, Measure> measureNameCond;

      MdschemaMeasuresRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(MDSCHEMA_MEASURES, request, handler);
         catalogCond = makeCondition(CATALOG_NAME_GETTER, CatalogName);
         schemaNameCond = makeCondition(SCHEMA_NAME_GETTER, SchemaName);
         cubeNameCond = makeCondition(ELEMENT_NAME_GETTER, CubeName);
         measureNameCond = makeCondition(ELEMENT_NAME_GETTER, MeasureName);
         measureUnameCond = makeCondition(ELEMENT_UNAME_GETTER, MeasureUniqueName);
      }

      private static final Column CatalogName = new Column("CATALOG_NAME", Type.String, null, Column.RESTRICTION, Column.OPTIONAL,
               "The name of the catalog to which this measure belongs.");
      private static final Column SchemaName = new Column("SCHEMA_NAME", Type.String, null, Column.RESTRICTION, Column.OPTIONAL,
               "The name of the schema to which this measure belongs.");
      private static final Column CubeName = new Column("CUBE_NAME", Type.String, null, Column.RESTRICTION, Column.OPTIONAL, "The name of the cube to which this measure belongs.");
      private static final Column CubeSource = new Column("CUBE_SOURCE", Type.UnsignedShort, null, Column.RESTRICTION, Column.OPTIONAL,
               "A bitmask with one of the following valid values: Cube, Dimension");
      private static final Column MeasureName = new Column("MEASURE_NAME", Type.String, null, Column.RESTRICTION, Column.OPTIONAL, "The name of the measure.");
      private static final Column MeasureUniqueName = new Column("MEASURE_UNIQUE_NAME", Type.String, null, Column.RESTRICTION, Column.OPTIONAL, "The Unique name of the measure.");

      private static final Column MeasureNameSql = new Column("MEASURE_NAME_SQL_COLUMN_NAME", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "The Unique name of the measure.");
      private static final Column MeasureCaption = new Column("MEASURE_CAPTION", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "A label or caption associated with the measure.");
      private static final Column MeasureGuid = new Column("MEASURE_GUID", Type.UUID, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "Measure GUID.");
      private static final Column MeasureAggregator = new Column("MEASURE_AGGREGATOR", Type.Integer, null, Column.NOT_RESTRICTION, Column.REQUIRED, "How a measure was derived.");
      private static final Column DataType = new Column("DATA_TYPE", Type.UnsignedShort, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "Data type of the measure.");
      private static final Column NumericPrecision = new Column("NUMERIC_PRECISION", Type.UnsignedShort, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "The maximum precision of the column for numeric data types " + "other than DBTYPE_VARNUMERIC.");
      private static final Column MeasureIsVisible = new Column("MEASURE_IS_VISIBLE", Type.Boolean, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "A Boolean that always returns True. If the measure is not " + "visible, it will not be included in the schema rowset.");
      private static final Column MeasureVisibility = new Column("MEASURE_VISIBILITY", Type.UnsignedShort, null, Column.RESTRICTION, Column.OPTIONAL,
               "A Boolean that always returns True. If the measure is not " + "visible, it will not be included in the schema rowset.");
      private static final Column LevelsList = new Column("LEVELS_LIST", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "A string that always returns NULL. EXCEPT that SQL Server " + "returns non-null values!!!");

      private static final Column MeasureUnqualifiedCaption = new Column("MEASURE_UNQUALIFIED_CAPTION", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");
      private static final Column Expression = new Column("EXPRESSION", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");

      private static final Column Description = new Column("DESCRIPTION", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "A human-readable description of the measure.");
      private static final Column FormatString = new Column("DEFAULT_FORMAT_STRING", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "The default format string for the measure.");
      private static final Column NumericScale = new Column("NUMERIC_SCALE", Type.Short, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "The number of digits to the right of the decimal point for " + "DBTYPE_DECIMAL, DBTYPE_NUMERIC, DBTYPE_VARNUMERIC. " + "Otherwise, this is NULL.");
      private static final Column MeasureUnits = new Column("MEASURE_UNITS", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");

      private static final Column MeasureGroupName = new Column("MEASUREGROUP_NAME", Type.String, null, Column.RESTRICTION, Column.OPTIONAL, "");
      private static final Column MeasureDisplayFolder = new Column("MEASURE_DISPLAY_FOLDER", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");

      public void populateImpl(XmlaResponse response, OlapConnection connection, List<Row> rows) throws XmlaException, SQLException {
         for (Catalog catalog : catIter(connection, catNameCond(), catalogCond)) {
            populateCatalog(connection, catalog, rows);
         }
      }

      protected void populateCatalog(OlapConnection connection, Catalog catalog, List<Row> rows) throws XmlaException, SQLException {
         // SQL Server actually includes the LEVELS_LIST row
         StringBuilder buf = new StringBuilder(100);

         for (Schema schema : filter(catalog.getSchemas(), schemaNameCond)) {
            for (Cube cube : filteredCubes(schema, cubeNameCond)) {
               buf.setLength(0);

               int j = 0;
               for (Dimension dimension : cube.getDimensions()) {
                  if (dimension.getDimensionType() == Dimension.Type.MEASURE) {
                     continue;
                  }
                  for (Hierarchy hierarchy : dimension.getHierarchies()) {
                     NamedList<Level> levels = hierarchy.getLevels();
                     Level lastLevel = levels.get(levels.size() - 1);
                     if (j++ > 0) {
                        buf.append(',');
                     }
                     buf.append(lastLevel.getUniqueName());
                  }
               }
               String levelListStr = buf.toString();

               List<Member> calcMembers = new ArrayList<Member>();
               for (Measure measure : filter(cube.getMeasures(), measureNameCond, measureUnameCond)) {
                  if (measure.isCalculated()) {
                     // Output calculated measures after stored
                     // measures.
                     calcMembers.add(measure);
                  } else {
                     populateMember(connection, catalog, measure, cube, levelListStr, rows);
                  }
               }

               for (Member member : calcMembers) {
                  populateMember(connection, catalog, member, cube, null, rows);
               }
            }
         }
      }

      private void populateMember(OlapConnection connection, Catalog catalog, Member member, Cube cube, String levelListStr, List<Row> rows) throws SQLException {
         Boolean visible = (Boolean) member.getPropertyValue(Property.StandardMemberProperty.$visible);
         if (visible == null) {
            visible = true;
         }
         if (!visible && !XmlaUtil.shouldEmitInvisibleMembers(request)) {
            return;
         }

         String desc = member.getDescription();
         if (desc == null) {
            desc = cube.getName() + " Cube - " + member.getName() + " Member";
         }
         String formatString = (String) member.getPropertyValue(Property.StandardCellProperty.FORMAT_STRING);
         if (formatString == null)
            formatString = "#.#";

         Row row = new Row();
         row.set(CatalogName.name, catalog.getName());
         row.set(CubeName.name, cube.getName());
         row.set(MeasureName.name, member.getName());
         row.set(MeasureUniqueName.name, member.getUniqueName());
         row.set(MeasureUnqualifiedCaption.name, member.getUniqueName());

         row.set(MeasureCaption.name, member.getCaption());
         // row.set(MeasureGuid.name, "");

         final CustomXmlaHandler.XmlaExtra extra = getExtra(connection);
         row.set(MeasureAggregator.name, extra.getMeasureAggregator(member));

         // DATA_TYPE DBType best guess is string
         XmlaConstants.DBType dbType = XmlaConstants.DBType.WSTR;
         String datatype = (String) member.getPropertyValue(Property.StandardCellProperty.DATATYPE);
         String precision = "16";
         if (datatype != null) {
            if (datatype.equals("Integer")) {
               dbType = XmlaConstants.DBType.I4;
               precision = "10";
            } else if (datatype.equals("Numeric")) {
               dbType = XmlaConstants.DBType.R8;
               precision = "16";
            } else {
               dbType = XmlaConstants.DBType.WSTR;
               precision = null;
            }
         }
         row.set(DataType.name, dbType.xmlaOrdinal());
         row.set(NumericPrecision.name, precision);
         row.set(NumericScale.name, -1);
         row.set(MeasureIsVisible.name, true);
         row.set(MeasureNameSql.name, member.getName());
         // row.set(MeasureGroupName.name, member.getCaption());
         // row.set(MeasureGroupName.name, "");
         row.set(Description.name, desc);
         row.set(MeasureDisplayFolder.name, "");
         row.set(FormatString.name, formatString);
         addRow(row, rows);
      }

      protected void setProperty(PropertyDefinition propertyDef, String value) {
         switch (propertyDef) {
         case Content:
            break;
         default:
            super.setProperty(propertyDef, value);
         }
      }
   }

   @SuppressWarnings("unused")
   static class MdschemaMembersRowset extends Rowset {
      private final Util.Functor1<Boolean, Catalog> catalogCond;
      private final Util.Functor1<Boolean, Schema> schemaNameCond;
      private final Util.Functor1<Boolean, Cube> cubeNameCond;
      private final Util.Functor1<Boolean, Dimension> dimensionUnameCond;
      private final Util.Functor1<Boolean, Hierarchy> hierarchyUnameCond;
      private final Util.Functor1<Boolean, Member> memberNameCond;
      private final Util.Functor1<Boolean, Member> memberUnameCond;
      private final Util.Functor1<Boolean, Member> memberTypeCond;

      MdschemaMembersRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(MDSCHEMA_MEMBERS, request, handler);
         catalogCond = makeCondition(CATALOG_NAME_GETTER, CatalogName);
         schemaNameCond = makeCondition(SCHEMA_NAME_GETTER, SchemaName);
         cubeNameCond = makeCondition(ELEMENT_NAME_GETTER, CubeName);
         dimensionUnameCond = makeCondition(ELEMENT_UNAME_GETTER, DimensionUniqueName);
         hierarchyUnameCond = makeCondition(ELEMENT_UNAME_GETTER, HierarchyUniqueName);
         memberNameCond = makeCondition(ELEMENT_NAME_GETTER, MemberName);
         memberUnameCond = makeCondition(ELEMENT_UNAME_GETTER, MemberUniqueName);
         memberTypeCond = makeCondition(MEMBER_TYPE_GETTER, MemberType);
      }

      private static final Column CatalogName = new Column("CATALOG_NAME", Type.String, null, Column.RESTRICTION, Column.OPTIONAL,
               "The name of the catalog to which this member belongs.");
      private static final Column SchemaName = new Column("SCHEMA_NAME", Type.String, null, Column.RESTRICTION, Column.OPTIONAL,
               "The name of the schema to which this member belongs.");
      private static final Column CubeName = new Column("CUBE_NAME", Type.String, null, Column.RESTRICTION, Column.REQUIRED, "Name of the cube to which this member belongs.");
      private static final Column DimensionUniqueName = new Column("DIMENSION_UNIQUE_NAME", Type.String, null, Column.RESTRICTION, Column.REQUIRED,
               "Unique name of the dimension to which this member belongs.");
      private static final Column HierarchyUniqueName = new Column("HIERARCHY_UNIQUE_NAME", Type.String, null, Column.RESTRICTION, Column.REQUIRED,
               "Unique name of the hierarchy. If the member belongs to more " + "than one hierarchy, there is one row for each hierarchy to " + "which it belongs.");
      private static final Column LevelUniqueName = new Column("LEVEL_UNIQUE_NAME", Type.String, null, Column.RESTRICTION, Column.REQUIRED,
               " Unique name of the level to which the member belongs.");
      private static final Column LevelNumber = new Column("LEVEL_NUMBER", Type.UnsignedInteger, null, Column.RESTRICTION, Column.REQUIRED,
               "The distance of the member from the root of the hierarchy.");
      private static final Column MemberOrdinal = new Column("MEMBER_ORDINAL", Type.UnsignedInteger, null, Column.NOT_RESTRICTION, Column.REQUIRED,
               "Ordinal number of the member. Sort rank of the member when " + "members of this dimension are sorted in their natural sort "
                        + "order. If providers do not have the concept of natural " + "ordering, this should be the rank when sorted by " + "MEMBER_NAME.");
      private static final Column MemberName = new Column("MEMBER_NAME", Type.String, null, Column.RESTRICTION, Column.REQUIRED, "Name of the member.");
      private static final Column MemberUniqueName = new Column("MEMBER_UNIQUE_NAME", Type.StringSometimesArray, null, Column.RESTRICTION, Column.REQUIRED,
               " Unique name of the member.");
      private static final Column MemberType = new Column("MEMBER_TYPE", Type.Integer, null, Column.RESTRICTION, Column.REQUIRED, "Type of the member.");
      private static final Column MemberGuid = new Column("MEMBER_GUID", Type.UUID, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "Memeber GUID.");
      private static final Column MemberCaption = new Column("MEMBER_CAPTION", Type.String, null, Column.RESTRICTION, Column.REQUIRED,
               "A label or caption associated with the member.");
      private static final Column ChildrenCardinality = new Column("CHILDREN_CARDINALITY", Type.UnsignedInteger, null, Column.NOT_RESTRICTION, Column.REQUIRED,
               "Number of children that the member has.");
      private static final Column ParentLevel = new Column("PARENT_LEVEL", Type.UnsignedInteger, null, Column.NOT_RESTRICTION, Column.REQUIRED,
               "The distance of the member's parent from the root level of " + "the hierarchy.");
      private static final Column ParentUniqueName = new Column("PARENT_UNIQUE_NAME", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "Unique name of the member's parent.");
      private static final Column ParentCount = new Column("PARENT_COUNT", Type.UnsignedInteger, null, Column.NOT_RESTRICTION, Column.REQUIRED,
               "Number of parents that this member has.");
      private static final Column TreeOp_ = new Column("TREE_OP", Type.Enumeration, Enumeration.TREE_OP, Column.RESTRICTION, Column.OPTIONAL, "Tree Operation");

      private static final Column CubeSource = new Column("CUBE_SOURCE", Type.UnsignedShort, null, Column.RESTRICTION, Column.OPTIONAL,
               "A bitmask with one of the following valid values: Cube, Dimension");
      private static final Column Scope = new Column("SCOPE", Type.Integer, null, Column.RESTRICTION, Column.OPTIONAL,
               "A bitmask with one of the following valid values: Cube, Dimension");
      /* Mondrian specified member properties. */
      private static final Column Depth = new Column("DEPTH", Type.Integer, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "depth");
      private static final Column MemberKey = new Column("MEMBER_KEY", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");
      private static final Column IsPlaceHolderMember = new Column("IS_PLACEHOLDERMEMBER", Type.Boolean, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");
      private static final Column IsDatamember = new Column("IS_DATAMEMBER", Type.Boolean, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");

      public void populateImpl(XmlaResponse response, OlapConnection connection, List<Row> rows) throws XmlaException, SQLException {
         for (Catalog catalog : catIter(connection, catNameCond(), catalogCond)) {
            populateCatalog(connection, catalog, rows);
         }
      }

      protected void populateCatalog(OlapConnection connection, Catalog catalog, List<Row> rows) throws XmlaException, SQLException {
         for (Schema schema : filter(catalog.getSchemas(), schemaNameCond)) {
            for (Cube cube : filteredCubes(schema, cubeNameCond)) {
               if (isRestricted(MemberUniqueName)) {

                  outputUniqueMemberName(connection, catalog, cube, rows);
               } else {
                  populateCube(connection, catalog, cube, rows);
               }
            }
         }
      }

      protected void populateCube(OlapConnection connection, Catalog catalog, Cube cube, List<Row> rows) throws XmlaException, SQLException {
         if (isRestricted(LevelUniqueName)) {
            // Note: If the LEVEL_UNIQUE_NAME has been specified, then
            // the dimension and hierarchy are specified implicitly.
            String levelUniqueName = getRestrictionValueAsString(LevelUniqueName);
            if (levelUniqueName == null) {
               // The query specified two or more unique names
               // which means that nothing will match.
               return;
            }

            Level level = lookupLevel(cube, levelUniqueName);
            if (level != null) {
               // Get members of this level, without access control, but
               // including calculated members.
               List<Member> members = level.getMembers();
               outputMembers(connection, members, catalog, cube, rows);
            }
         } else {
            for (Dimension dimension : filter(cube.getDimensions(), dimensionUnameCond)) {
               populateDimension(connection, catalog, cube, dimension, rows);
            }
         }
      }

      protected void populateDimension(OlapConnection connection, Catalog catalog, Cube cube, Dimension dimension, List<Row> rows) throws XmlaException, SQLException {
         for (Hierarchy hierarchy : filter(dimension.getHierarchies(), hierarchyUnameCond)) {
            populateHierarchy(connection, catalog, cube, hierarchy, rows);
         }
      }

      protected void populateHierarchy(OlapConnection connection, Catalog catalog, Cube cube, Hierarchy hierarchy, List<Row> rows) throws XmlaException, SQLException {
         if (isRestricted(LevelNumber)) {
            int levelNumber = getRestrictionValueAsInt(LevelNumber);
            if (levelNumber == -1) {
               LOGGER.warn("RowsetDefinition.populateHierarchy: " + "LevelNumber invalid");
               return;
            }
            NamedList<Level> levels = hierarchy.getLevels();
            if (levelNumber >= levels.size()) {
               LOGGER.warn("RowsetDefinition.populateHierarchy: " + "LevelNumber (" + levelNumber + ") is greater than number of levels (" + levels.size() + ") for hierarchy \""
                        + hierarchy.getUniqueName() + "\"");
               return;
            }

            Level level = levels.get(levelNumber);
            List<Member> members = level.getMembers();
            outputMembers(connection, members, catalog, cube, rows);
         } else {
            // At this point we get ALL of the members associated with
            // the Hierarchy (rather than getting them one at a time).
            // The value returned is not used at this point but they are
            // now cached in the SchemaReader.
            for (Level level : hierarchy.getLevels()) {
               outputMembers(connection, level.getMembers(), catalog, cube, rows);
            }
         }
      }

      /**
       * Returns whether a value contains all of the bits in a mask.
       */
      private static boolean mask(int value, int mask) {
         return (value & mask) == mask;
      }

      /**
       * Adds a member to a result list and, depending upon the
       * <code>treeOp</code> parameter, other relatives of the member. This
       * method recursively invokes itself to walk up, down, or across the
       * hierarchy.
       */
      private void populateMember(OlapConnection connection, Catalog catalog, Cube cube, Member member, int treeOp, List<Row> rows) throws SQLException {
         // Visit node itself.
         if (mask(treeOp, TreeOp.SELF.xmlaOrdinal())) {
            outputMember(connection, member, catalog, cube, rows);
         }
         // Visit node's siblings (not including itself).
         if (mask(treeOp, TreeOp.SIBLINGS.xmlaOrdinal())) {
            final List<Member> siblings;
            final Member parent = member.getParentMember();
            if (parent == null) {
               siblings = member.getHierarchy().getRootMembers();
            } else {
               siblings = Olap4jUtil.cast(parent.getChildMembers());
            }
            for (Member sibling : siblings) {
               if (sibling.equals(member)) {
                  continue;
               }
               populateMember(connection, catalog, cube, sibling, TreeOp.SELF.xmlaOrdinal(), rows);
            }
         }
         // Visit node's descendants or its immediate children, but not both.
         if (mask(treeOp, TreeOp.DESCENDANTS.xmlaOrdinal())) {
            for (Member child : member.getChildMembers()) {
               populateMember(connection, catalog, cube, child, TreeOp.SELF.xmlaOrdinal() | TreeOp.DESCENDANTS.xmlaOrdinal(), rows);
            }
         } else if (mask(treeOp, TreeOp.CHILDREN.xmlaOrdinal())) {
            for (Member child : member.getChildMembers()) {
               populateMember(connection, catalog, cube, child, TreeOp.SELF.xmlaOrdinal(), rows);
            }
         }
         // Visit node's ancestors or its immediate parent, but not both.
         if (mask(treeOp, TreeOp.ANCESTORS.xmlaOrdinal())) {
            final Member parent = member.getParentMember();
            if (parent != null) {
               populateMember(connection, catalog, cube, parent, TreeOp.SELF.xmlaOrdinal() | TreeOp.ANCESTORS.xmlaOrdinal(), rows);
            }
         } else if (mask(treeOp, TreeOp.PARENT.xmlaOrdinal())) {
            final Member parent = member.getParentMember();
            if (parent != null) {
               populateMember(connection, catalog, cube, parent, TreeOp.SELF.xmlaOrdinal(), rows);
            }
         }
      }

      protected ArrayList<Column> pruneRestrictions(ArrayList<Column> list) {
         // If they've restricted TreeOp, we don't want to literally filter
         // the result on TreeOp (because it's not an output column) or
         // on MemberUniqueName (because TreeOp will have caused us to
         // generate other members than the one asked for).
         if (list.contains(TreeOp_)) {
            list.remove(TreeOp_);
            list.remove(MemberUniqueName);
         }
         return list;
      }

      private void outputMembers(OlapConnection connection, List<Member> members, final Catalog catalog, Cube cube, List<Row> rows) throws SQLException {
         for (Member member : members) {
            outputMember(connection, member, catalog, cube, rows);
         }
      }

      private void outputUniqueMemberName(final OlapConnection connection, final Catalog catalog, Cube cube, List<Row> rows) throws SQLException {
         final Object unameRestrictions = restrictions.get(MemberUniqueName.name);
         List<String> list;
         if (unameRestrictions instanceof String) {
            list = Collections.singletonList((String) unameRestrictions);
         } else {
            list = (List<String>) unameRestrictions;
         }
         for (String memberUniqueName : list) {
            final IdentifierNode identifierNode = IdentifierNode.parseIdentifier(memberUniqueName);
            Member member = cube.lookupMember(identifierNode.getSegmentList());
            if (member == null) {
               return;
            }
            if (isRestricted(TreeOp_)) {
               int treeOp = getRestrictionValueAsInt(TreeOp_);
               if (treeOp == -1) {
                  return;
               }
               populateMember(connection, catalog, cube, member, treeOp, rows);
            } else {
               outputMember(connection, member, catalog, cube, rows);
            }
         }
      }

      private void outputMember(OlapConnection connection, Member member, final Catalog catalog, Cube cube, List<Row> rows) throws SQLException {
         if (!memberNameCond.apply(member)) {
            return;
         }
         if (!memberTypeCond.apply(member)) {
            return;
         }

         getExtra(connection).checkMemberOrdinal(member);

         // Check whether the member is visible, otherwise do not dump.
         Boolean visible = (Boolean) member.getPropertyValue(Property.StandardMemberProperty.$visible);
         if (visible == null) {
            visible = true;
         }
         if (!visible && !XmlaUtil.shouldEmitInvisibleMembers(request)) {
            return;
         }

         final Level level = member.getLevel();
         final Hierarchy hierarchy = level.getHierarchy();
         final Dimension dimension = hierarchy.getDimension();

         int adjustedLevelDepth = level.getDepth();

         Row row = new Row();
         row.set(CatalogName.name, catalog.getName());
         row.set(SchemaName.name, cube.getSchema().getName());
         row.set(CubeName.name, cube.getName());
         row.set(DimensionUniqueName.name, dimension.getUniqueName());
         row.set(HierarchyUniqueName.name, hierarchy.getUniqueName());
         row.set(LevelUniqueName.name, level.getUniqueName());
         row.set(LevelNumber.name, adjustedLevelDepth);
         row.set(MemberOrdinal.name, member.getOrdinal());
         row.set(MemberName.name, member.getName());
         row.set(MemberUniqueName.name, member.getUniqueName());
         row.set(MemberType.name, member.getMemberType().ordinal());
         // row.set(MemberGuid.name, "");
         row.set(MemberCaption.name, member.getCaption());
         row.set(ChildrenCardinality.name, member.getPropertyValue(Property.StandardMemberProperty.CHILDREN_CARDINALITY));
         row.set(ChildrenCardinality.name, 100);

         if (adjustedLevelDepth == 0) {
            row.set(ParentLevel.name, 0);
         } else {
            row.set(ParentLevel.name, adjustedLevelDepth - 1);
            final Member parentMember = member.getParentMember();
            if (parentMember != null) {
               row.set(ParentUniqueName.name, parentMember.getUniqueName());
            }
         }

         row.set(ParentCount.name, member.getParentMember() == null ? 0 : 1);

         row.set(Depth.name, member.getDepth());
         row.set(MemberKey.name, member.getCaption());// Member_Key =
                                                      // Member_Caption?
         row.set(IsPlaceHolderMember.name, false);
         row.set(IsDatamember.name, false);
         addRow(row, rows);
      }

      protected void setProperty(PropertyDefinition propertyDef, String value) {
         switch (propertyDef) {
         case Content:
            break;
         default:
            super.setProperty(propertyDef, value);
         }
      }
   }

   static class MdschemaSetsRowset extends Rowset {
      private final Util.Functor1<Boolean, Catalog> catalogCond;
      private final Util.Functor1<Boolean, Schema> schemaNameCond;
      private final Util.Functor1<Boolean, Cube> cubeNameCond;
      private final Util.Functor1<Boolean, NamedSet> setUnameCond;
      private static final String GLOBAL_SCOPE = "1";

      MdschemaSetsRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(MDSCHEMA_SETS, request, handler);
         catalogCond = makeCondition(CATALOG_NAME_GETTER, CatalogName);
         schemaNameCond = makeCondition(SCHEMA_NAME_GETTER, SchemaName);
         cubeNameCond = makeCondition(ELEMENT_NAME_GETTER, CubeName);
         setUnameCond = makeCondition(ELEMENT_UNAME_GETTER, SetName);
      }

      private static final Column CatalogName = new Column("CATALOG_NAME", Type.String, null, true, true, null);
      private static final Column SchemaName = new Column("SCHEMA_NAME", Type.String, null, true, true, null);
      private static final Column CubeName = new Column("CUBE_NAME", Type.String, null, true, true, null);
      private static final Column CubeSource = new Column("CUBE_SOURCE", Type.UnsignedShort, null, true, true, null);

      private static final Column SetName = new Column("SET_NAME", Type.String, null, true, true, null);
      private static final Column Scope = new Column("SCOPE", Type.Integer, null, true, true, null);
      private static final Column Description = new Column("DESCRIPTION", Type.String, null, false, true, "A human-readable description of the measure.");
      private static final Column Expression = new Column("EXPRESSION", Type.String, null, false, true, "");
      private static final Column Dimensions = new Column("DIMENSIONS", Type.String, null, false, true, "");
      private static final Column SetCaption = new Column("SET_CAPTION", Type.String, null, true, true, null);

      private static final Column SetDisplayFolder = new Column("SET_DISPLAY_FOLDER", Type.String, null, false, true, "");
      private static final Column SetEvaluationContext = new Column("SET_EVALUATION_CONTEXT", Type.Integer, null, true, true, "");
      private static final Column HierarchyUniqueName = new Column("HIERARCHY_UNIQUE_NAME", Type.String, null, true, true, "");


      public void populateImpl(XmlaResponse response, OlapConnection connection, List<Row> rows) throws XmlaException, OlapException {
         for (Catalog catalog : catIter(connection, catNameCond(), catalogCond)) {
            processCatalog(connection, catalog, rows);
         }
      }

      private void processCatalog(OlapConnection connection, Catalog catalog, List<Row> rows) throws OlapException {
         for (Schema schema : filter(catalog.getSchemas(), schemaNameCond)) {
            for (Cube cube : filter(sortedCubes(schema), cubeNameCond)) {
               populateNamedSets(cube, catalog, rows);
            }
         }
      }

      private void populateNamedSets(Cube cube, Catalog catalog, List<Row> rows) {
         for (NamedSet namedSet : filter(cube.getSets(), setUnameCond)) {
            Row row = new Row();

            Writer writer = new StringWriter();
            ParseTreeWriter parseTreeWriter = new ParseTreeWriter(writer);
            ParseTreeNode node = namedSet.getExpression();
            node.unparse(parseTreeWriter);
            // String expression = writer.toString();
            // expression = expression.substring(1,
            // expression.toCharArray().length - 1);

            final String expression = "[Product].[Category].&[1]";

            row.set(CatalogName.name, catalog.getName());
            // row.set(SchemaName.name, cube.getSchema().getName());
            row.set(CubeName.name, cube.getName());
            row.set(SetName.name, namedSet.getUniqueName().substring(1, namedSet.getUniqueName().length() - 1));
            row.set(Scope.name, GLOBAL_SCOPE);
            row.set(Description.name, namedSet.getDescription());
            row.set(Expression.name, expression);
            row.set(Dimensions.name, "[Store].[USA].[CA]");
            row.set(SetCaption.name, namedSet.getCaption());
            row.set(SetDisplayFolder.name, "");
            row.set(SetEvaluationContext.name, 2);
            addRow(row, rows);
         }
      }
   }

   static class MdschemaKpisRowset extends Rowset {
      MdschemaKpisRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(MDSCHEMA_KPIS, request, handler);
      }

      private static final Column CatalogName = new Column("CATALOG_NAME", Type.String, null, Column.RESTRICTION, Column.OPTIONAL, "");
      private static final Column SchemaName = new Column("SCHEMA_NAME", Type.String, null, Column.RESTRICTION, Column.OPTIONAL, "");
      private static final Column CubeName = new Column("CUBE_NAME", Type.String, null, Column.RESTRICTION, Column.OPTIONAL, "");
      private static final Column KpiName = new Column("KPI_NAME", Type.String, null, Column.RESTRICTION, Column.OPTIONAL, "");
      private static final Column CubeSource = new Column("CUBE_SOURCE", Type.UnsignedShort, null, Column.RESTRICTION, Column.OPTIONAL, "");
      private static final Column Scope = new Column("SCOPE", Type.Integer, null, Column.RESTRICTION, Column.OPTIONAL, "");

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {

      }
   }

   static class MdschemaMeasureGroupRowset extends Rowset {
      MdschemaMeasureGroupRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(MDSCHEMA_MEASUREGROUPS, request, handler);

      }

      private static final Column CatalogName = new Column("CATALOG_NAME", Type.String, null, true, true, null);
      private static final Column SchemaName = new Column("SCHEMA_NAME", Type.String, null, true, true, null);
      private static final Column CubeName = new Column("CUBE_NAME", Type.String, null, true, false, null);
      private static final Column MeasureGroupName = new Column("MEASUREGROUP_NAME", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");
      private static final Column Description = new Column("DESCRIPTION", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "A human-readable description of the measure.");
      private static final Column IsWriteEnabled = new Column("IS_WRITE_ENABLED", Type.Boolean, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "The unique name of the dimension.");
      private static final Column MeasureGroupCaption = new Column("MEASUREGROUP_NAME", Type.String, null, Column.RESTRICTION, Column.OPTIONAL, "");
      private static final Column CubeSource = new Column("CUBE_NAME", Type.UnsignedShort, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {

      }
   }

   static class MdschemaMeasureGroupDimensionRowset extends Rowset {
      MdschemaMeasureGroupDimensionRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(MDSCHEMA_MEASUREGROUP_DIMENSIONS, request, handler);

      }

      private static final Column CatalogName = new Column("CATALOG_NAME", Type.String, null, true, true, null);
      private static final Column SchemaName = new Column("SCHEMA_NAME", Type.String, null, true, true, null);
      private static final Column CubeName = new Column("CUBE_NAME", Type.String, null, true, false, null);
      private static final Column MeasureGroupName = new Column("MEASUREGROUP_NAME", Type.String, null, Column.RESTRICTION, Column.OPTIONAL, "");
      private static final Column MeasureGroupCardinality = new Column("MEASUREGROUP_CARDINALITY", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");
      private static final Column DimensionUniqueName = new Column("DIMENSION_UNIQUE_NAME", Type.String, null, Column.RESTRICTION, Column.OPTIONAL,
               "The unique name of the dimension.");
      private static final Column DimensionCardinality = new Column("DIMENSION_CARDINALITY", Type.UnsignedInteger, null, Column.NOT_RESTRICTION, Column.REQUIRED,
               "The number of members in the key attribute.");
      private static final Column DimensionVisibility = new Column("DIMENSION_VISIBILITY", Type.UnsignedShort, null, Column.RESTRICTION, Column.OPTIONAL, "Always TRUE.");
      private static final Column DimensionISFactDimension = new Column("DIMENSION_IS_FACT_DIMENSION", Type.Boolean, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "Always TRUE.");
      private static final Column DimensionPath = new Column("DIMENSION_PATH", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "Always TRUE.");

      private static final Column DimensionGranularity = new Column("DIMENSION_GRANULARITY", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "Always TRUE.");

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {
         // TODO Auto-generated method stub

      }
   }

   static class DiscoverTransactionsRowset extends Rowset {
      DiscoverTransactionsRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(DISCOVER_TRANSACTIONS, request, handler);

      }

      private static final Column TransactionId = new Column("TRANSACTION_ID", Type.String, null, true, false, null);
      private static final Column TransactionSessionId = new Column("TRANSACTION_SESSION_ID", Type.String, null, true, false, null);

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {
         // TODO Auto-generated method stub

      }
   }

   static class DiscoverDbConnections extends Rowset {
      DiscoverDbConnections(XmlaRequest request, CustomXmlaHandler handler) {
         super(DISCOVER_DB_CONNECTIONS, request, handler);

      }

      private static final Column ConnectionId = new Column("CONNECTION_ID", Type.Integer, null, true, true, null);
      private static final Column ConnectionInUse = new Column("CONNECTION_IN_USE", Type.Integer, null, true, true, null);
      private static final Column ConnectionServerName = new Column("CONNECTION_SERVER_NAME", Type.String, null, true, true, null);
      private static final Column ConnectionCatalogName = new Column("CONNECTION_CATALOG_NAME", Type.String, null, true, true, null);

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {
         // TODO Auto-generated method stub

      }
   }

   static class DiscoverMasterKey extends Rowset {
      DiscoverMasterKey(XmlaRequest request, CustomXmlaHandler handler) {
         super(DISCOVER_MASTER_KEY, request, handler);

      }

      private static final Column Key = new Column("Key", Type.String, null, true, false, null);

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {
         // TODO Auto-generated method stub

      }
   }

   static class DiscoverPerformanceCounterSchema extends Rowset {
      DiscoverPerformanceCounterSchema(XmlaRequest request, CustomXmlaHandler handler) {
         super(DISCOVER_PERFORMANCE_COUNTERS, request, handler);

      }

      private static final Column PrefCounterName = new Column("PERF_COUNTER_NAME", Type.String, null, true, false, null);

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {
         // TODO Auto-generated method stub

      }
   }

   static class DiscoverPartitionStateSchema extends Rowset {
      DiscoverPartitionStateSchema(XmlaRequest request, CustomXmlaHandler handler) {
         super(DISCOVER_PARTITION_STAT, request, handler);

      }

      private static final Column DatabaseName = new Column("DATABASE_NAME", Type.String, null, true, false, null);
      private static final Column CubeName = new Column("CUBE_NAME", Type.String, null, true, false, null);
      private static final Column MeasureGroupName = new Column("MEASURE_GROUP_NAME", Type.String, null, true, false, null);
      private static final Column PartitionName = new Column("PARTITION_NAME", Type.String, null, true, false, null);

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {
         // TODO Auto-generated method stub

      }
   }

   static class DiscoverPartitionDimensionStatSchema extends Rowset {
      DiscoverPartitionDimensionStatSchema(XmlaRequest request, CustomXmlaHandler handler) {
         super(DISCOVER_PARTITION_DIMENSION_STAT, request, handler);

      }

      private static final Column DatabaseName = new Column("DATABASE_NAME", Type.String, null, true, false, null);
      private static final Column CubeName = new Column("CUBE_NAME", Type.String, null, true, false, null);
      private static final Column MeasureGroupName = new Column("MEASURE_GROUP_NAME", Type.String, null, true, false, null);
      private static final Column PartitionName = new Column("PARTITION_NAME", Type.String, null, true, false, null);

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {
         // TODO Auto-generated method stub

      }
   }

   static class DiscoverDBConnectionsRowset extends Rowset {
      DiscoverDBConnectionsRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(DISCOVER_DB_CONNECTIONS, request, handler);

      }

      private static final Column ConnectionId = new Column("CONNECTION_ID", Type.Integer, null, true, false, null);
      private static final Column ConnectionInUser = new Column("CONNECTION_IN_USE", Type.Integer, null, true, false, null);
      private static final Column ConnectionServerName = new Column("CONNECTION_SERVER_NAME", Type.String, null, true, false, null);
      private static final Column ConnectionCatalogName = new Column("CONNECTION_CATALOG_NAME", Type.String, null, true, false, null);
      private static final Column ConnectionSpId = new Column("CONNECTION_SPID", Type.Integer, null, true, false, null);

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {
         // TODO Auto-generated method stub

      }
   }

   static class DiscoverJobsRowset extends Rowset {
      DiscoverJobsRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(DISCOVER_JOBS, request, handler);

      }

      private static final Column SpId = new Column("SPID", Type.Integer, null, true, false, null);
      private static final Column JobId = new Column("JOB_ID", Type.Integer, null, true, false, null);
      private static final Column JobDescription = new Column("JOB_DESCRIPTION", Type.String, null, true, false, null);
      private static final Column JobThreadPollId = new Column("JOB_THREADPOOL_ID", Type.Integer, null, true, false, null);
      private static final Column JobMinTotalTimeMs = new Column("JOB_MIN_TOTAL_TIME_MS", Type.Long, null, true, false, null);

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {
         // TODO Auto-generated method stub

      }
   }

   static class DiscoverSessionsRowset extends Rowset {
      DiscoverSessionsRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(DISCOVER_SESSIONS, request, handler);

      }

      private static final Column SessionId = new Column("SESSION_ID", Type.String, null, true, false, null);
      private static final Column SessionSpId = new Column("SESSION_SPID", Type.Integer, null, true, false, null);
      private static final Column SessionConnectionId = new Column("SESSION_CONNECTION_ID", Type.Integer, null, true, false, null);
      private static final Column SessionUserName = new Column("SESSION_USER_NAME", Type.String, null, true, false, null);
      private static final Column SessionCurrentDatabase = new Column("SESSION_CURRENT_DATABASE", Type.String, null, true, false, null);
      private static final Column SessionElapsedTimeMs = new Column("SESSION_ELAPSED_TIME_MS", Type.UnsignedLong, null, true, false, null);
      private static final Column SessionCpuTimeMs = new Column("SESSION_CPU_TIME_MS", Type.UnsignedLong, null, true, false, null);
      private static final Column SessionIdleTimeMs = new Column("SESSION_IDLE_TIME_MS", Type.UnsignedLong, null, true, false, null);
      private static final Column SessionStatus = new Column("SESSION_STATUS", Type.Integer, null, true, false, null);

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {
         // TODO Auto-generated method stub

      }
   }

   static class DiscoverConnectionsRowset extends Rowset {
      DiscoverConnectionsRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(DISCOVER_CONNECTIONS, request, handler);

      }

      private static final Column ConnectionId = new Column("CONNECTION_ID", Type.Integer, null, true, false, null);
      private static final Column ConnectionUserName = new Column("CONNECTION_USER_NAME", Type.String, null, true, false, null);
      private static final Column ConnectionImpersonatedUserName = new Column("CONNECTION_IMPERSONATED_USER_NAME", Type.String, null, true, false, null);
      private static final Column ConnectionHostName = new Column("CONNECTION_HOST_NAME", Type.String, null, true, false, null);
      private static final Column ConnectionElapsedTiemMs = new Column("CONNECTION_ELAPSED_TIME_MS", Type.Long, null, true, false, null);
      private static final Column ConnectionLastCommandElapsedTimeMs = new Column("CONNECTION_LAST_COMMAND_ELAPSED_TIME_MS", Type.Long, null, true, false, null);
      private static final Column ConnectionIdleTimeMs = new Column("CONNECTION_IDLE_TIME_MS", Type.Long, null, true, false, null);

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {
         // TODO Auto-generated method stub

      }
   }

   static class DiscoverKeyWordsSchema extends Rowset {
      DiscoverKeyWordsSchema(XmlaRequest request, CustomXmlaHandler handler) {
         super(DISCOVER_KEYWORDS, request, handler);

      }

      private static final Column Keyword = new Column("Keyword", Type.String, null, true, false, null);

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {
         // TODO Auto-generated method stub

      }
   }

   static class DiscoverEnumeratorsSchema extends Rowset {
      DiscoverEnumeratorsSchema(XmlaRequest request, CustomXmlaHandler handler) {
         super(DISCOVER_ENUMERATORS, request, handler);

      }

      private static final Column EnumName = new Column("EnumName", Type.String, null, true, false, null);

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {
         // TODO Auto-generated method stub

      }
   }

   static class MdschemaInputDataSourcesSchema extends Rowset {
      MdschemaInputDataSourcesSchema(XmlaRequest request, CustomXmlaHandler handler) {
         super(MDSCHEMA_INPUT_DATASOURCES, request, handler);

      }

      private static final Column CatalogName = new Column("CATALOG_NAME", Type.String, null, true, false, null);
      private static final Column SchemaName = new Column("SCHEMA_NAME", Type.String, null, true, false, null);
      private static final Column DataSourceName = new Column("DATASOURCE_NAME", Type.String, null, true, false, null);
      private static final Column DataSourceType = new Column("DATASOURCE_TYPE", Type.String, null, true, false, null);

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {
         // TODO Auto-generated method stub

      }
   }

   static class DmschemaMiningServices extends Rowset {
      DmschemaMiningServices(XmlaRequest request, CustomXmlaHandler handler) {
         super(DMSCHEMA_MINING_SERVICES, request, handler);

      }

      private static final Column ServiceName = new Column("SERVICE_NAME", Type.String, null, true, false, null);
      private static final Column ServiceTypeId = new Column("SERVICE_TYPE_ID", Type.UnsignedInteger, null, true, false, null);

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {
         // TODO Auto-generated method stub

      }
   }

   static class DmschemaMiningServiceParametersRowset extends Rowset {
      DmschemaMiningServiceParametersRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(DMSCHEMA_MINING_SERVICES, request, handler);

      }

      private static final Column ServiceName = new Column("SERVICE_NAME", Type.String, null, true, false, null);
      private static final Column ParameterName = new Column("PARAMETER_NAME", Type.String, null, true, false, null);

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {
         // TODO Auto-generated method stub

      }
   }

   static class DmschemaMiningFunctionsRowset extends Rowset {
      DmschemaMiningFunctionsRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(DMSCHEMA_MINING_FUNCTIONS, request, handler);

      }

      private static final Column ServiceName = new Column("SERVICE_NAME", Type.String, null, true, false, null);
      private static final Column FunctionName = new Column("FUNCTION_NAME", Type.String, null, true, false, null);

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {
         // TODO Auto-generated method stub

      }
   }

   static class DmschemaMiningModelXmlRowset extends Rowset {

      DmschemaMiningModelXmlRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(DMSCHEMA_MINING_MODEL_XML, request, handler);

      }

      private static final Column ModelCatalog = new Column("MODEL_CATALOG", Type.String, null, true, false, null);
      private static final Column ModelSchema = new Column("MODEL_SCHEMA", Type.String, null, true, false, null);
      private static final Column ModelName = new Column("MODEL_NAME", Type.String, null, true, false, null);
      private static final Column ModelType = new Column("MODEL_TYPE", Type.String, null, true, false, null);

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {
         // TODO Auto-generated method stub

      }
   }

   static class DbschemaMiningColumnsRowset extends Rowset {

      DbschemaMiningColumnsRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(DMSCHEMA_MINING_COLUMNS, request, handler);

      }

      private static final Column ModelCatalog = new Column("MODEL_CATALOG", Type.String, null, true, false, null);
      private static final Column ModelSchema = new Column("MODEL_SCHEMA", Type.String, null, true, false, null);
      private static final Column ModelName = new Column("MODEL_NAME", Type.String, null, true, false, null);
      private static final Column ColumnName = new Column("COLUMN_NAME", Type.String, null, true, false, null);

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {
         // TODO Auto-generated method stub

      }
   }

   // DbschemaMiningStructuresRowset
   static class DbschemaMiningStructuresRowset extends Rowset {

      DbschemaMiningStructuresRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(DMSCHEMA_MINING_STRUCTURES, request, handler);

      }

      private static final Column StructureCatalog = new Column("STRUCTURE_CATALOG", Type.String, null, true, false, null);
      private static final Column StructureSchema = new Column("STRUCTURE_SCHEMA", Type.String, null, true, false, null);
      private static final Column StructureName = new Column("STRUCTURE_NAME", Type.String, null, true, false, null);

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {
         // TODO Auto-generated method stub

      }
   }

   // DMSCHEMA_MINING_STRUCTURE_COLUMNS
   static class DbschemaMiningStructuresColumnsRowset extends Rowset {

      DbschemaMiningStructuresColumnsRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(DMSCHEMA_MINING_STRUCTURE_COLUMNS, request, handler);

      }

      private static final Column StructureCatalog = new Column("STRUCTURE_CATALOG", Type.String, null, true, false, null);
      private static final Column StructureSchema = new Column("STRUCTURE_SCHEMA", Type.String, null, true, false, null);
      private static final Column StructureName = new Column("STRUCTURE_NAME", Type.String, null, true, false, null);
      private static final Column ColumnName = new Column("COLUMN_NAME", Type.String, null, true, false, null);

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {
         // TODO Auto-generated method stub

      }
   }

   // DMSCHEMA_MINING_MODEL_CONTENT_PMML
   static class DbschemaMiningCodelContentPmmlRowset extends Rowset {

      DbschemaMiningCodelContentPmmlRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(DMSCHEMA_MINING_MODEL_CONTENT_PMML, request, handler);

      }

      private static final Column ModelCatalog = new Column("MODEL_CATALOG", Type.String, null, true, false, null);
      private static final Column ModelSchema = new Column("MODEL_SCHEMA", Type.String, null, true, false, null);
      private static final Column ModelName = new Column("MODEL_NAME", Type.String, null, true, false, null);
      private static final Column ModelType = new Column("MODEL_TYPE", Type.String, null, true, false, null);

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {
         // TODO Auto-generated method stub

      }
   }

   // DMSCHEMA_MINING_MODEL_CONTENT_PMML
   static class DbschemaMiningModelsRowset extends Rowset {

      DbschemaMiningModelsRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(DMSCHEMA_MINING_MODELS, request, handler);

      }

      private static final Column ModelCatalog = new Column("MODEL_CATALOG", Type.String, null, true, false, null);
      private static final Column ModelSchema = new Column("MODEL_SCHEMA", Type.String, null, true, false, null);
      private static final Column ModelName = new Column("MODEL_NAME", Type.String, null, true, false, null);
      private static final Column ModelType = new Column("MODEL_TYPE", Type.String, null, true, false, null);
      private static final Column ServiceName = new Column("SERVICE_NAME", Type.String, null, true, false, null);
      private static final Column ServiceTypeId = new Column("SERVICE_TYPE_ID", Type.UnsignedInteger, null, true, false, null);
      private static final Column MiningStructure = new Column("MINING_STRUCTURE", Type.String, null, true, false, null);

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {
         // TODO Auto-generated method stub

      }
   }

   static class DmschemaMiningModelContentRowset extends Rowset {
      DmschemaMiningModelContentRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(DMSCHEMA_MINING_MODEL_CONTENT, request, handler);

      }

      private static final Column ModelCatalog = new Column("MODEL_CATALOG", Type.String, null, true, false, null);
      private static final Column ModelSchema = new Column("MODEL_SCHEMA", Type.String, null, true, false, null);
      private static final Column ModelName = new Column("MODEL_NAME", Type.String, null, true, false, null);

      private static final Column AttributeName = new Column("ATTRIBUTE_NAME", Type.String, null, true, false, null);
      private static final Column NodeName = new Column("NODE_NAME", Type.String, null, true, false, null);

      private static final Column NodeUniqueName = new Column("NODE_UNIQUE_NAME", Type.String, null, true, false, null);
      private static final Column NodeType = new Column("NODE_TYPE", Type.Integer, null, true, false, null);

      private static final Column NodeGuid = new Column("NODE_GUID", Type.String, null, true, false, null);
      private static final Column NodeCaption = new Column("NODE_CAPTION", Type.String, null, true, false, null);

      private static final Column TreeOperation = new Column("TREE_OPERATION", Type.UnsignedInteger, null, true, false, null);

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {
         // TODO Auto-generated method stub

      }
   }

   static class DiscoverXmlMetaData extends Rowset {
      DiscoverXmlMetaData(XmlaRequest request, CustomXmlaHandler handler) {
         super(DISCOVER_XML_METADATA, request, handler);

      }

      private static final Column DatabaseID = new Column("DatabaseID", Type.String, null, true, false, null);
      private static final Column DimensionID = new Column("DimensionID", Type.String, null, true, false, null);
      private static final Column CubeID = new Column("CubeID", Type.String, null, true, false, null);
      private static final Column MeasureGroupID = new Column("MeasureGroupID", Type.String, null, true, false, null);
      private static final Column PartitionID = new Column("PartitionID", Type.String, null, true, false, null);
      private static final Column PerspectiveID = new Column("PerspectiveID", Type.String, null, true, false, null);
      private static final Column DimensionPermissionID = new Column("DimensionPermissionID", Type.String, null, true, false, null);
      private static final Column RoleID = new Column("RoleID", Type.String, null, true, false, null);
      private static final Column DatabasePermissionID = new Column("DatabasePermissionID", Type.String, null, true, false, null);
      private static final Column MiningModelID = new Column("MiningModelID", Type.String, null, true, false, null);
      private static final Column MiningModelPermissionID = new Column("MiningModelPermissionID", Type.String, null, true, false, null);
      private static final Column DataSourceID = new Column("DataSourceID", Type.String, null, true, false, null);
      private static final Column MiningStructureID = new Column("MiningStructureID", Type.String, null, true, false, null);
      private static final Column AggregationDesignID = new Column("AggregationDesignID", Type.String, null, true, false, null);
      private static final Column TraceID = new Column("TraceID", Type.String, null, true, false, null);
      private static final Column MiningStructurePermissionID = new Column("MiningStructurePermissionID", Type.String, null, true, false, null);
      private static final Column CubePermissionID = new Column("CubePermissionID", Type.String, null, true, false, null);
      private static final Column AssemblyID = new Column("AssemblyID", Type.String, null, true, false, null);
      private static final Column MdxScriptID = new Column("MdxScriptID", Type.String, null, true, false, null);
      private static final Column DataSourceViewID = new Column("DataSourceViewID", Type.String, null, true, false, null);
      private static final Column DataSourcePermissionID = new Column("DataSourcePermissionID", Type.String, null, true, false, null);
      private static final Column CalculatedColumns = new Column("CalculatedColumns", Type.String, null, true, false, null);
      private static final Column ObjectExpansion = new Column("ObjectExpansion", Type.String, null, true, false, null);

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {
         // TODO Auto-generated method stub

      }
   }

   static class DiscoverLocksRowset extends Rowset {
      DiscoverLocksRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(DISCOVER_LOCKS, request, handler);

      }

      private static final Column SPID = new Column("SPID", Type.Integer, null, true, false, null);
      private static final Column LockTransactionId = new Column("LOCK_TRANSACTION_ID", Type.UUID, null, true, false, null);
      private static final Column LockObjectId = new Column("LOCK_OBJECT_ID", Type.String, null, true, false, null);
      private static final Column LockStauts = new Column("LOCK_STATUS", Type.Integer, null, true, false, null);
      private static final Column LockType = new Column("LOCK_TYPE", Type.Integer, null, true, false, null);
      private static final Column LockMinTotalMs = new Column("LOCK_MIN_TOTAL_MS", Type.Long, null, true, false, null);

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {
         // TODO Auto-generated method stub

      }
   }

   static class DiscoverLocationsRowset extends Rowset {
      DiscoverLocationsRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(DISCOVER_LOCATIONS, request, handler);

      }

      private static final Column LocationBackUpFilePathName = new Column("LOCATION_BACKUP_FILE_PATHNAME", Type.String, null, true, false, null);
      private static final Column LocationPassWord = new Column("LOCATION_PASSWORD", Type.String, null, true, false, null);

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {
         // TODO Auto-generated method stub

      }
   }

   static class DiscoverDimensionStatRowset extends Rowset {
      DiscoverDimensionStatRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(DISCOVER_DIMENSION_STAT, request, handler);

      }

      private static final Column DatabaseName = new Column("DATABASE_NAME", Type.String, null, true, false, null);
      private static final Column DimensionName = new Column("DIMENSION_NAME", Type.String, null, true, false, null);

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {
         // TODO Auto-generated method stub

      }
   }

   static class DiscoverCommandsRowset extends Rowset {
      DiscoverCommandsRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(DISCOVER_COMMANDS, request, handler);

      }

      private static final Column SessionSpId = new Column("SESSION_SPID", Type.Integer, null, true, false, null);

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {
         // TODO Auto-generated method stub

      }
   }

   static class DiscoverStorageTablesRowset extends Rowset {
      DiscoverStorageTablesRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(DISCOVER_STORAGE_TABLES, request, handler);

      }

      private static final Column DatabaseName = new Column("DATABASE_NAME", Type.String, null, true, false, null);
      private static final Column CubeName = new Column("CUBE_NAME", Type.String, null, true, false, null);
      private static final Column MeasureGroupName = new Column("MEASURE_GROUP_NAME", Type.String, null, true, false, null);

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {
         // TODO Auto-generated method stub

      }
   }

   static class DiscoverStorageTableColumnsRowset extends Rowset {
      DiscoverStorageTableColumnsRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(DISCOVER_STORAGE_TABLE_COLUMNS, request, handler);

      }

      private static final Column DatabaseName = new Column("DATABASE_NAME", Type.String, null, true, false, null);
      private static final Column CubeName = new Column("CUBE_NAME", Type.String, null, true, false, null);
      private static final Column MeasureGroupName = new Column("MEASURE_GROUP_NAME", Type.String, null, true, false, null);

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {
         // TODO Auto-generated method stub

      }
   }

   static class DiscoverStorageTableColumnSegments extends Rowset {
      DiscoverStorageTableColumnSegments(XmlaRequest request, CustomXmlaHandler handler) {
         super(DISCOVER_STORAGE_TABLE_COLUMN_SEGMENTS, request, handler);

      }

      private static final Column DatabaseName = new Column("DATABASE_NAME", Type.String, null, true, false, null);
      private static final Column CubeName = new Column("CUBE_NAME", Type.String, null, true, false, null);
      private static final Column MeasureGroupName = new Column("MEASURE_GROUP_NAME", Type.String, null, true, false, null);
      private static final Column PartitionName = new Column("PARTITION_NAME", Type.String, null, true, false, null);

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {
         // TODO Auto-generated method stub

      }
   }

   static class DiscoverCommandObjectsRowset extends Rowset {
      DiscoverCommandObjectsRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(DISCOVER_COMMAND_OBJECTS, request, handler);

      }

      private static final Column SessionSpId = new Column("SESSION_SPID", Type.Integer, null, true, false, null);
      private static final Column SessionId = new Column("SESSION_ID", Type.String, null, true, false, null);
      private static final Column ObjectParentPath = new Column("OBJECT_PARENT_PATH", Type.String, null, true, false, null);
      private static final Column ObjectId = new Column("OBJECT_ID", Type.String, null, true, false, null);

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {
         // TODO Auto-generated method stub

      }
   }

   static class DiscoverCalcDependencyRowset extends Rowset {
      DiscoverCalcDependencyRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(DISCOVER_CALC_DEPENDENCY, request, handler);

      }

      private static final Column DatabaseName = new Column("DATABASE_NAME", Type.String, null, true, false, null);
      private static final Column ObjectType = new Column("OBJECT_TYPE", Type.String, null, true, false, null);
      private static final Column Query = new Column("Query", Type.String, null, true, false, null);

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {
         // TODO Auto-generated method stub

      }
   }

   static class DiscoverCsdlMetaData extends Rowset {
      DiscoverCsdlMetaData(XmlaRequest request, CustomXmlaHandler handler) {
         super(DISCOVER_CSDL_METADATA, request, handler);

      }

      private static final Column CatalogName = new Column("CATALOG_NAME", Type.String, null, true, false, null);
      private static final Column PerspectiveName = new Column("PERSPECTIVE_NAME", Type.String, null, true, false, null);
      private static final Column Version = new Column("VERSION", Type.String, null, true, false, null);

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {
         // TODO Auto-generated method stub

      }
   }

   static class DiscoverObjectActivityRowset extends Rowset {
      DiscoverObjectActivityRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(DISCOVER_OBJECT_ACTIVITY, request, handler);

      }

      private static final Column ObjectParentPath = new Column("OBJECT_PARENT_PATH", Type.String, null, true, false, null);
      private static final Column ObjectId = new Column("OBJECT_ID", Type.String, null, true, false, null);

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {
         // TODO Auto-generated method stub

      }
   }

   static class DiscoverObjectMemoryUsageRowset extends Rowset {
      DiscoverObjectMemoryUsageRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(DISCOVER_OBJECT_MEMORY_USAGE, request, handler);

      }

      private static final Column ObjectParentPath = new Column("OBJECT_PARENT_PATH", Type.String, null, true, false, null);
      private static final Column ObjectId = new Column("OBJECT_ID", Type.String, null, true, false, null);

      @Override
      protected void populateImpl(XmlaResponse response, OlapConnection connection, List rows) throws XmlaException, SQLException {
         // TODO Auto-generated method stub

      }
   }

   @SuppressWarnings("unused")
   static class MdschemaPropertiesRowset extends Rowset {
      private Util.Functor1<Boolean, Catalog> catalogCond;
      private Util.Functor1<Boolean, Schema> schemaNameCond;
      private Util.Functor1<Boolean, Cube> cubeNameCond;
      private Util.Functor1<Boolean, Dimension> dimensionUnameCond;
      private Util.Functor1<Boolean, Hierarchy> hierarchyUnameCond;
      private Util.Functor1<Boolean, Property> propertyNameCond;
      private Util.Functor1<Boolean, Level> levelNameCond;

      MdschemaPropertiesRowset(XmlaRequest request, CustomXmlaHandler handler) {
         super(MDSCHEMA_PROPERTIES, request, handler);
         catalogCond = makeCondition(CATALOG_NAME_GETTER, CatalogName);
         schemaNameCond = makeCondition(SCHEMA_NAME_GETTER, SchemaName);
         cubeNameCond = makeCondition(ELEMENT_NAME_GETTER, CubeName);
         dimensionUnameCond = makeCondition(ELEMENT_UNAME_GETTER, DimensionUniqueName);
         hierarchyUnameCond = makeCondition(ELEMENT_UNAME_GETTER, HierarchyUniqueName);
         levelNameCond = makeCondition(ELEMENT_NAME_GETTER, LevelUniqueName);
         propertyNameCond = makeCondition(ELEMENT_NAME_GETTER, PropertyName);
      }

      private static final Column CatalogName = new Column("CATALOG_NAME", Type.String, null, Column.RESTRICTION, Column.OPTIONAL, "The name of the database.");
      private static final Column SchemaName = new Column("SCHEMA_NAME", Type.String, null, Column.RESTRICTION, Column.OPTIONAL,
               "The name of the schema to which this property belongs.");
      private static final Column CubeName = new Column("CUBE_NAME", Type.String, null, Column.RESTRICTION, Column.OPTIONAL, "The name of the cube.");
      private static final Column DimensionUniqueName = new Column("DIMENSION_UNIQUE_NAME", Type.String, null, Column.RESTRICTION, Column.OPTIONAL,
               "The unique name of the dimension.");
      private static Column HierarchyUniqueName = new Column("HIERARCHY_UNIQUE_NAME", Type.String, null, Column.RESTRICTION, Column.OPTIONAL, "The unique name of the hierarchy.");
      private static final Column LevelUniqueName = new Column("LEVEL_UNIQUE_NAME", Type.String, null, Column.RESTRICTION, Column.OPTIONAL,
               "The unique name of the level to which this property belongs.");
      // According to MS this should not be nullable
      private static final Column MemberUniqueName = new Column("MEMBER_UNIQUE_NAME", Type.String, null, Column.RESTRICTION, Column.OPTIONAL,
               "The unique name of the member to which the property belongs.");
      private static final Column PropertyName = new Column("PROPERTY_NAME", Type.String, null, Column.RESTRICTION, Column.OPTIONAL, "Name of the property.");
      private static final Column PropertyType = new Column("PROPERTY_TYPE", Type.Short, null, Column.RESTRICTION, Column.OPTIONAL,
               "A bitmap that specifies the type of the property");
      private static final Column PropertyCaption = new Column("PROPERTY_CAPTION", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "A label or caption associated with the property, used " + "primarily for display purposes.");
      private static final Column DataType = new Column("DATA_TYPE", Type.UnsignedShort, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "Data type of the property.");
      private static final Column NumericPrecission = new Column("NUMERIC_PRECISION", Type.UnsignedShort, null, Column.RESTRICTION, Column.OPTIONAL, "Data type of the property.");

      private static final Column PropertyContentType = new Column("PROPERTY_CONTENT_TYPE", Type.Short, null, Column.RESTRICTION, Column.OPTIONAL, "The type of the property.");

      private static final Column PropertyOrigin = new Column("PROPERTY_ORIGIN", Type.UnsignedShort, null, Column.RESTRICTION, Column.OPTIONAL, "The type of the property.");
      private static final Column PropertyAttributeHierarchyName = new Column("PROPERTY_ATTRIBUTE_HIERARCHY_NAME", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL, ".");
      private static final Column PropertyCardinality = new Column("PROPERTY_CARDINALITY", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL, ".");

      private static final Column Description = new Column("DESCRIPTION", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL,
               "A human-readable description of the measure.");
      private static final Column PropertyVisibility = new Column("PROPERTY_VISIBILITY", Type.UnsignedShort, null, Column.RESTRICTION, Column.OPTIONAL, "");

      private static final Column LevelOrderingProperty = new Column("LEVEL_ORDERING_PROPERTY", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");
      private static final Column LevelDbtype = new Column("LEVEL_DBTYPE", Type.Integer, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");
      private static final Column LevelNameSqlColumnName = new Column("LEVEL_NAME_SQL_COLUMN_NAME", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");
      private static final Column LevelKeySqlColumnName = new Column("LEVEL_KEY_SQL_COLUMN_NAME", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");
      private static final Column LevelUniqueNameSqlColumnName = new Column("LEVEL_UNIQUE_NAME_SQL_COLUMN_NAME", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");
      private static final Column LevelAttributeHierarchyName = new Column("LEVEL_ATTRIBUTE_HIERARCHY_NAME", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");
      private static final Column LevelKeyCardinality = new Column("LEVEL_KEY_CARDINALITY", Type.UnsignedShort, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");
      private static final Column LevelOrign = new Column("LEVEL_ORIGIN", Type.UnsignedShort, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");
      private static final Column CubeSource = new Column("CUBE_SOURCE", Type.UnsignedShort, null, Column.RESTRICTION, Column.OPTIONAL, "");
      private static final Column CharacterMaximumLength = new Column("CHARACTER_MAXIMUM_LENGTH", Type.UnsignedInteger, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");
      private static final Column CharacterOctetLength = new Column("CHARACTER_OCTET_LENGTH", Type.UnsignedInteger, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");
      private static final Column NumericPrecision = new Column("NUMERIC_PRECISION", Type.UnsignedShort, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");
      private static final Column NumericScale = new Column("NUMERIC_SCALE", Type.UnsignedShort, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");
      private static final Column SqlColumnName = new Column("SQL_COLUMN_NAME", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");
      private static final Column Language = new Column("LANGUAGE", Type.UnsignedShort, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");
      private static final Column MimeType = new Column("MIME_TYPE", Type.String, null, Column.NOT_RESTRICTION, Column.OPTIONAL, "");

      protected boolean needConnection() {
         return false;
      }

      public void populateImpl(XmlaResponse response, OlapConnection connection, List<Row> rows) throws XmlaException, SQLException {
         // Default PROPERTY_TYPE is MDPROP_MEMBER.
         final List<String> list = (List<String>) restrictions.get(PropertyType.name);
         Set<Property.TypeFlag> typeFlags;
         if (list == null) {
            typeFlags = Olap4jUtil.enumSetOf(Property.TypeFlag.MEMBER);
         } else {
            typeFlags = Property.TypeFlag.getDictionary().forMask(Integer.valueOf(list.get(0)));
         }

         for (Property.TypeFlag typeFlag : typeFlags) {
            switch (typeFlag) {
            case MEMBER:
               populateMember(rows);
               break;
            case CELL:
               populateCell(rows);
               break;
            case SYSTEM:
            case BLOB:
            default:
               break;
            }
         }
      }

      private void populateCell(List<Row> rows) {
         List<Row> tmpRow = new ArrayList<Row>(12);
         for (int i = 0; i < 12; i++) {
            tmpRow.add(new Row());
         }
         for (Property.StandardCellProperty property : Property.StandardCellProperty.values()) {
            if (property.name().equals("VALUE") || property.name().equals("FORMAT_STRING") || property.name().equals("BACK_COLOR") || property.name().equals("FORE_COLOR")
                     || property.name().equals("FONT_NAME") || property.name().equals("FONT_SIZE") || property.name().equals("FONT_FLAGS") || property.name().equals("LANGUAGE")
                     || property.name().equals("CELL_ORDINAL") || property.name().equals("FORMATTED_VALUE") || property.name().equals("ACTION_TYPE")
                     || property.name().equals("UPDATEABLE")) {

               Row row = new Row();
               row.set(PropertyType.name, Property.TypeFlag.getDictionary().toMask(property.getType()));
               row.set(PropertyName.name, property.name());

               row.set(PropertyCaption.name, property.getCaption());

               if (property.name().equals("BACK_COLOR")) {
                  row.set(DataType.name, 19);

               } else if (property.name().equals("FORE_COLOR")) {
                  row.set(DataType.name, 19);

               } else if (property.name().equals("ACTION_TYPE")) {
                  row.set(DataType.name, 19);

               } else if (property.name().equals("FONT_SIZE")) {
                  row.set(DataType.name, 18);

               } else if (property.name().equals("FONT_FLAGS")) {
                  row.set(DataType.name, 3);

               } else {
                  row.set(DataType.name, property.getDatatype().xmlaOrdinal());
               }
               // row.set(PropertyOrigin.name, "6");
               // row.set(PropertyIsVisible.name, true);
               if (property.name().equals("VALUE"))
                  tmpRow.set(0, row);
               else if (property.name().equals("FORMAT_STRING"))
                  tmpRow.set(1, row);
               else if (property.name().equals("BACK_COLOR"))
                  tmpRow.set(2, row);
               else if (property.name().equals("FORE_COLOR"))
                  tmpRow.set(3, row);
               else if (property.name().equals("FONT_NAME"))
                  tmpRow.set(4, row);
               else if (property.name().equals("FONT_SIZE"))
                  tmpRow.set(5, row);
               else if (property.name().equals("FONT_FLAGS"))
                  tmpRow.set(6, row);
               else if (property.name().equals("LANGUAGE"))
                  tmpRow.set(7, row);
               else if (property.name().equals("CELL_ORDINAL"))
                  tmpRow.set(8, row);
               else if (property.name().equals("FORMATTED_VALUE"))
                  tmpRow.set(9, row);
               else if (property.name().equals("ACTION_TYPE"))
                  tmpRow.set(10, row);
               else if (property.name().equals("UPDATEABLE"))
                  tmpRow.set(11, row);

            }

         }
         for (Row r : tmpRow) {
            addRow(r, rows);

         }
      }

      private void populateMember(List<Row> rows) throws SQLException {
         OlapConnection connection = handler.getConnection(request, Collections.<String, String> emptyMap());
         for (Catalog catalog : catIter(connection, catNameCond(), catalogCond)) {
            populateCatalog(catalog, rows);
         }
      }

      protected void populateCatalog(Catalog catalog, List<Row> rows) throws XmlaException, SQLException {
         for (Schema schema : filter(catalog.getSchemas(), schemaNameCond)) {
            for (Cube cube : filteredCubes(schema, cubeNameCond)) {
               populateCube(catalog, cube, rows);
            }
         }
      }

      protected void populateCube(Catalog catalog, Cube cube, List<Row> rows) throws XmlaException, SQLException {
         if (cube instanceof SharedDimensionHolderCube) {
            return;
         }
         if (isRestricted(LevelUniqueName)) {
            // Note: If the LEVEL_UNIQUE_NAME has been specified, then
            // the dimension and hierarchy are specified implicitly.
            String levelUniqueName = getRestrictionValueAsString(LevelUniqueName);
            if (levelUniqueName == null) {
               // The query specified two or more unique names
               // which means that nothing will match.
               return;
            }
            Level level = lookupLevel(cube, levelUniqueName);
            if (!levelUniqueName.contains("].[") && levelUniqueName.startsWith("[") && levelUniqueName.endsWith("]")) {
               hierarchyUnameCond = makeCondition(ELEMENT_UNAME_GETTER, HierarchyUniqueName);

               for (Dimension dimension : filter(cube.getDimensions(), dimensionUnameCond)) {
                  populateDimension(catalog, cube, dimension, rows);
               }
               return;

            } else if (level == null) {
               return;
            }
            populateLevel(catalog, cube, level, rows);
         } else {
            for (Dimension dimension : filter(cube.getDimensions(), dimensionUnameCond)) {
               populateDimension(catalog, cube, dimension, rows);
            }
         }
      }

      private void populateDimension(Catalog catalog, Cube cube, Dimension dimension, List<Row> rows) throws SQLException {
         for (Hierarchy hierarchy : filter(dimension.getHierarchies(), hierarchyUnameCond)) {
            populateHierarchy(catalog, cube, hierarchy, rows);
         }
      }

      private void populateHierarchy(Catalog catalog, Cube cube, Hierarchy hierarchy, List<Row> rows) throws SQLException {
         for (Level level : hierarchy.getLevels()) {
            populateLevel(catalog, cube, level, rows);
         }
      }

      private void populateLevel(Catalog catalog, Cube cube, Level level, List<Row> rows) throws SQLException {
         final CustomXmlaHandler.XmlaExtra extra = getExtra(catalog.getMetaData().getConnection());
         for (Property property : filter(extra.getLevelProperties(level), propertyNameCond)) {
            if (extra.isPropertyInternal(property)) {
               continue;
            }
            outputProperty(property, catalog, cube, level, rows);
         }
      }

      private void outputProperty(Property property, Catalog catalog, Cube cube, Level level, List<Row> rows) {
         Hierarchy hierarchy = level.getHierarchy();
         Dimension dimension = hierarchy.getDimension();

         String propertyName = property.getName();

         Row row = new Row();
         row.set(CatalogName.name, catalog.getName());
         row.set(CubeName.name, cube.getName());
         row.set(DimensionUniqueName.name, dimension.getUniqueName());
         row.set(HierarchyUniqueName.name, hierarchy.getUniqueName());
         row.set(LevelUniqueName.name, level.getUniqueName());

         row.set(PropertyName.name, propertyName);
         row.set(PropertyCaption.name, property.getCaption());

         // Only member properties now
         row.set(PropertyType.name, 1);
         row.set(PropertyContentType.name, Property.ContentType.REGULAR.xmlaOrdinal());
         XmlaConstants.DBType dbType = getDBTypeFromProperty(property);
         row.set(DataType.name, 0);

         row.set(PropertyOrigin.name, 1);

         String desc = cube.getName() + " Cube - " + getHierarchyName(hierarchy) + " Hierarchy - " + level.getName() + " Level - " + property.getName() + " Property";
         row.set(Description.name, desc);
         row.set(PropertyCardinality.name, "MANY");
         row.set(PropertyAttributeHierarchyName.name, propertyName);
         row.set(PropertyVisibility.name, 1);
         addRow(row, rows);
      }

      protected void setProperty(PropertyDefinition propertyDef, String value) {
         switch (propertyDef) {
         case Content:
            break;
         default:
            super.setProperty(propertyDef, value);
         }
      }
   }

   public static final Util.Functor1<String, Catalog> CATALOG_NAME_GETTER = new Util.Functor1<String, Catalog>() {
      public String apply(Catalog catalog) {
         return catalog.getName();
      }
   };

   public static final Util.Functor1<String, Schema> SCHEMA_NAME_GETTER = new Util.Functor1<String, Schema>() {
      public String apply(Schema schema) {
         return schema.getName();
      }
   };

   public static final Util.Functor1<String, MetadataElement> ELEMENT_NAME_GETTER = new Util.Functor1<String, MetadataElement>() {
      public String apply(MetadataElement element) {
         return element.getName();
      }
   };

   public static final Util.Functor1<String, MetadataElement> ELEMENT_UNAME_GETTER = new Util.Functor1<String, MetadataElement>() {
      public String apply(MetadataElement element) {
         return element.getUniqueName();
      }
   };

   public static final Util.Functor1<Member.Type, Member> MEMBER_TYPE_GETTER = new Util.Functor1<Member.Type, Member>() {
      public Member.Type apply(Member member) {
         return member.getMemberType();
      }
   };

   public static final Util.Functor1<String, PropertyDefinition> PROPDEF_NAME_GETTER = new Util.Functor1<String, PropertyDefinition>() {
      public String apply(PropertyDefinition property) {
         return property.name();
      }
   };

   static void serialize(StringBuilder buf, Collection<String> strings) {
      int k = 0;
      for (String name : Util.sort(strings)) {
         if (k++ > 0) {
            buf.append(',');
         }
         buf.append(name);
      }
   }

   private static Level lookupLevel(Cube cube, String levelUniqueName) {
      for (Dimension dimension : cube.getDimensions()) {
         for (Hierarchy hierarchy : dimension.getHierarchies()) {
            for (Level level : hierarchy.getLevels()) {
               if (level.getUniqueName().equals(levelUniqueName)) {
                  return level;
               }
            }
         }
      }
      return null;
   }

   static Iterable<Cube> sortedCubes(Schema schema) throws OlapException {
      return Util.sort(schema.getCubes(), new Comparator<Cube>() {
         public int compare(Cube o1, Cube o2) {
            return o1.getName().compareTo(o2.getName());
         }
      });
   }

   static Iterable<Cube> filteredCubes(final Schema schema, Util.Functor1<Boolean, Cube> cubeNameCond) throws OlapException {
      final Iterable<Cube> iterable = filter(sortedCubes(schema), cubeNameCond);
      if (!cubeNameCond.apply(new SharedDimensionHolderCube(schema))) {
         return iterable;
      }
      return Composite.of(Collections.singletonList(new SharedDimensionHolderCube(schema)), iterable);
   }

   private static String getHierarchyName(Hierarchy hierarchy) {
      String hierarchyName = hierarchy.getName();
      if (MondrianProperties.instance().SsasCompatibleNaming.get() && !hierarchyName.equals(hierarchy.getDimension().getName())) {
         hierarchyName = hierarchy.getDimension().getName() + "." + hierarchyName;
      }
      return hierarchyName;
   }

   private static XmlaRequest wrapRequest(XmlaRequest request, Map<Column, String> map) {
      final Map<String, Object> restrictionsMap = new HashMap<String, Object>(request.getRestrictions());
      for (Map.Entry<Column, String> entry : map.entrySet()) {
         restrictionsMap.put(entry.getKey().name, Collections.singletonList(entry.getValue()));
      }

      return new DelegatingXmlaRequest(request) {
         @Override
         public Map<String, Object> getRestrictions() {
            return restrictionsMap;
         }
      };
   }

   /**
    * Returns an iterator over the catalogs in a connection, setting the
    * connection's catalog to each successful catalog in turn.
    * 
    * @param connection
    *           Connection
    * @param conds
    *           Zero or more conditions to be applied to catalogs
    * @return Iterator over catalogs
    */
   public static Iterable<Catalog> catIter(final OlapConnection connection, final Util.Functor1<Boolean, Catalog>... conds) {
      return new Iterable<Catalog>() {
         public Iterator<Catalog> iterator() {
            try {
               return new Iterator<Catalog>() {
                  final Iterator<Catalog> catalogIter = Util.filter(connection.getOlapCatalogs(), conds).iterator();

                  public boolean hasNext() {
                     return catalogIter.hasNext();
                  }

                  public Catalog next() {
                     Catalog catalog = catalogIter.next();
                     try {
                        connection.setCatalog(catalog.getName());
                     } catch (SQLException e) {
                        throw new RuntimeException(e);
                     }
                     return catalog;
                  }

                  public void remove() {
                     throw new UnsupportedOperationException();
                  }
               };
            } catch (OlapException e) {
               throw new RuntimeException("Failed to obtain a list of catalogs form the connection object.", e);
            }
         }
      };
   }

   public static Iterable<Catalog> catIter(final OlapConnection connection) {
      return new Iterable<Catalog>() {
         public Iterator<Catalog> iterator() {
            try {
               return new Iterator<Catalog>() {
                  final Iterator<Catalog> catalogIter = Util.filter(connection.getOlapCatalogs()).iterator();

                  public boolean hasNext() {
                     return catalogIter.hasNext();
                  }

                  public Catalog next() {
                     Catalog catalog = catalogIter.next();
                     try {
                        connection.setCatalog(catalog.getName());
                     } catch (SQLException e) {
                        throw new RuntimeException(e);
                     }
                     return catalog;
                  }

                  public void remove() {
                     throw new UnsupportedOperationException();
                  }
               };
            } catch (OlapException e) {
               throw new RuntimeException("Failed to obtain a list of catalogs form the connection object.", e);
            }
         }
      };
   }

   private static class DelegatingXmlaRequest implements XmlaRequest {
      protected final XmlaRequest request;

      public DelegatingXmlaRequest(XmlaRequest request) {
         this.request = request;
      }

      public XmlaConstants.Method getMethod() {
         return request.getMethod();
      }

      public Map<String, String> getProperties() {
         return request.getProperties();
      }

      public Map<String, Object> getRestrictions() {
         return request.getRestrictions();
      }

      public String getStatement() {
         return request.getStatement();
      }

      public String getRoleName() {
         return request.getRoleName();
      }

      public String getRequestType() {
         return request.getRequestType();
      }

      public boolean isDrillThrough() {
         return request.isDrillThrough();
      }

      public String getUsername() {
         return request.getUsername();
      }

      public String getPassword() {
         return request.getPassword();
      }

      public String getSessionId() {
         return request.getSessionId();
      }
   }

   /**
    * Dummy implementation of {@link Cube} that holds all shared dimensions in a
    * given schema. Less error-prone than requiring all generator code to cope
    * with a null Cube.
    */
   private static class SharedDimensionHolderCube implements Cube {
      private final Schema schema;

      public SharedDimensionHolderCube(Schema schema) {
         this.schema = schema;
      }

      public Schema getSchema() {
         return schema;
      }

      public NamedList<Dimension> getDimensions() {
         try {
            return schema.getSharedDimensions();
         } catch (OlapException e) {
            throw new RuntimeException(e);
         }
      }

      public NamedList<Hierarchy> getHierarchies() {
         final NamedList<Hierarchy> hierarchyList = new ArrayNamedListImpl<Hierarchy>() {

            private static final long serialVersionUID = -5177781397505125769L;

            public String getName(Object hierarchy) {
               return ((Hierarchy) hierarchy).getName();
            }
         };
         for (Dimension dimension : getDimensions()) {
            hierarchyList.addAll(dimension.getHierarchies());
         }
         return hierarchyList;
      }

      public List<Measure> getMeasures() {
         return Collections.emptyList();
      }

      public NamedList<NamedSet> getSets() {
         throw new UnsupportedOperationException();
      }

      public Collection<Locale> getSupportedLocales() {
         throw new UnsupportedOperationException();
      }

      public Member lookupMember(List<IdentifierSegment> identifierSegments) throws org.olap4j.OlapException {
         throw new UnsupportedOperationException();
      }

      public List<Member> lookupMembers(Set<Member.TreeOp> treeOps, List<IdentifierSegment> identifierSegments) throws org.olap4j.OlapException {
         throw new UnsupportedOperationException();
      }

      public boolean isDrillThroughEnabled() {
         return false;
      }

      public String getName() {
         return "";
      }

      public String getUniqueName() {
         return "";
      }

      public String getCaption() {
         return "";
      }

      public String getDescription() {
         return "";
      }

      public boolean isVisible() {
         return false;
      }
   }
}

// End RowsetDefinition.java
