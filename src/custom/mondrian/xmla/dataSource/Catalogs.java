/*
 * Copyright (c) 2008-2014 Open Link Financial, Inc. All Rights Reserved.
 */

package custom.mondrian.xmla.dataSource;

import java.util.ArrayList;
import java.util.List;


/**
 * Entity class for <Catalogs> element in data source configuration file.
 */

public class Catalogs extends DataSourceElement{
   
   private List<Catalog> catalogs;
  
   public Catalogs(){
   }
   
   
   
   /**
    * @return xml string example:
    <Catalogs>
         <Catalog name="FoodMart1">
            <DataSourceInfo>Provider=mondrian;Jdbc="jdbc:adssql:///jdbc_2.4.0";JdbcDrivers=com.olf.ads.jdbc.ADSDriver;Catalog=/WEB-INF/schemas/FoodMart1.xml;connectionPooling=false</DataSourceInfo>
            <Definition>/WEB-INF/schemas/FoodMart1.xml</Definition>
         </Catalog>
         <Catalog name="FoodMart2">
            <DataSourceInfo>Provider=mondrian;Jdbc="jdbc:adssql:///jdbc_2.4.0";JdbcDrivers=com.olf.ads.jdbc.ADSDriver;Catalog=/WEB-INF/schemas/FoodMart2.xml;connectionPooling=false</DataSourceInfo>
            <Definition>/WEB-INF/schemas/FoodMart2.xml</Definition>
         </Catalog>
   </Catalogs>
    */
   @Override
   public String getElementXmlString() {
      StringBuilder catalogsStrBuilder = new StringBuilder();
      catalogsStrBuilder.append("<Catalogs>");
      for(Catalog catalog: catalogs){
         catalogsStrBuilder.append("\n"+ catalog.getElementXmlString());
      }
      
      catalogsStrBuilder.append("</Catalogs>");
      return catalogsStrBuilder.toString();
   }
   

   
   public void addCatalog(Catalog catalog){
      if(catalogs == null)
           catalogs = new ArrayList<Catalog>();
      catalogs.add(catalog);
     
   }
   
   public List<Catalog> getCatalogs() {
      if(this.catalogs != null)
           return catalogs;
      return (List<Catalog>)new ArrayList<Catalog>();
   }
   

   

}
