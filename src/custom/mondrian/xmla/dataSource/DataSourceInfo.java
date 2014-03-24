/*
 * Copyright (c) 2008-2013 Open Link Financial, Inc. All Rights Reserved.
 */

package custom.mondrian.xmla.dataSource;

import java.util.Properties;

/**
 * DataSoruceInfo defines the element <DataSourceInfo> in datasource config file.
 * It's made up by: Provider, Jdbc, JdbcDrivers, Catalog, and additional connection properties.
 */

public class DataSourceInfo extends DataSourceElement {
   
   /*provider name, by default it's mondrian*/
   private String provider;
   
   /** URI used to connect to OLAP */
   private String jdbcUri;
   private String jdbcDriver;
   private Properties properties;
   private String catalogFullName;
   
   /**Constructor with default setting: provider name = "mondrian";  connectionPooling = "false"*/
   public DataSourceInfo(String jdbcUri, String jdbcDriver){ 
      this.provider = "mondrian";
      this.jdbcUri = jdbcUri;
      this.jdbcDriver = jdbcDriver;
      properties = new Properties();
      properties.put("connectionPooling", "false");
      
   }
   
   public DataSourceInfo(String provider, String jdbcUri, String jdbcDriver){
      this.provider = provider;
      this.jdbcUri = jdbcUri;
      this.jdbcDriver = jdbcDriver;
   }
   

   
   public String getElementXmlString() {
      
      return "<DataSourceInfo>Provider=" + provider + ";Jdbc=" + jdbcUri +
               ";JdbcDrivers=" + jdbcDriver + ";Catalog=" + catalogFullName + ";" + getPropertiesString(properties) + "</DataSourceInfo>";
   
   }
   

   
   /**
    * 
    * @param properties
    * @return string properties.toString() 
    */
   public String getPropertiesString(Properties properties) {
      StringBuilder strBuilder = new StringBuilder(properties.toString());
      strBuilder.delete(0, 1);
      strBuilder.delete(strBuilder.length() -1, strBuilder.length());
      return strBuilder.toString();
   }
   
   public void setCatalogFullName(String catalogFullName){
      this.catalogFullName= catalogFullName;
   }
}
