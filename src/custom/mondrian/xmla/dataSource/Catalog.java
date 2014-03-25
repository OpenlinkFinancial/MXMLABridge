/*
 * Copyright (c) 2008-2014 Open Link Financial, Inc. All Rights Reserved.
 */

package custom.mondrian.xmla.dataSource;

/**
 * Entity class for element <Catalog> of datasource.xml
 */
class Catalog extends DataSourceElement{
   
   /**Name of catalog e.g. CashFlow*/
   private String name;
   /**Full name of catalog in file system CashFlow.xml*/
   private String fileName;
   /**Relative path to catalog directory */
   private String dir;
   /** In the xml formart: <Definition>[catalog_dir] \ [fileName]</Definition> */
  
   private DataSourceInfo dsInfo;
   
   public Catalog(String fileName, String dir, DataSourceInfo dsInfo) {
      this.fileName = fileName;
      this.dir = dir;
      this.dsInfo = dsInfo;
      dsInfo.setCatalogFullName(dir + "/" + fileName);
      setName();
      
   }
   
   public String getElementXmlString() {
      String definitionXmlPattern = "<Definition>" + dir+"/" + fileName + "</Definition>";
      String result = "<Catalog name=\"" + name +"\">\n"+
               this.dsInfo.getElementXmlString() + "\n"+
                  definitionXmlPattern + "\n" +
               "</Catalog>";
      
      return result;
   }
   
   private void setName() {
      if(this.fileName != null && this.fileName.length() > 4 && this.fileName.endsWith(".xml")){
         this.name = fileName.substring(0, fileName.length() - 4);
      }
   }
   public String getName() {
      return this.name;
   }
   
   /**
    * @return String represents relative path with file name to the the catalog file
    * Example return: /WEB-INF/schemas/FoodMart1.xml
    */
   public String getFullName() {
      return this.dir + "/" + this.fileName;
   }
   
   public DataSourceInfo getDatasourceInfo(){
      return this.dsInfo;
   }
   
   
   
   
   
}
