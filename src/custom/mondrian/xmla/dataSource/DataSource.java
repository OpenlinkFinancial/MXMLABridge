/*
 * Copyright (c) 2008-2013 Open Link Financial, Inc. All Rights Reserved.
 */

package custom.mondrian.xmla.dataSource;

/**
 * Entity class matches content of datasource.xml
 */

public class DataSource {
  
   /** uri used to connect to OLAP server*/
   private String uri;
   /** default data source info used by mondrian*/
   private DataSourceInfo initialDsInfo;
   
   private Catalog initialCatalog;
   private Catalogs catalogs;
   
   public DataSource(Catalogs catalogs, String uri){
      this.setCatalogs(catalogs);
      this.uri = uri;

   }

   public Catalogs getCatalogs() {
      return catalogs;
   }

   public void setCatalogs(Catalogs catalogs) {
      this.catalogs = catalogs;
      if(initialDsInfo == null && catalogs.getCatalogs().size() > 0){
         this.initialCatalog = catalogs.getCatalogs().get(0);
         this.initialDsInfo = initialCatalog.getDatasourceInfo();
      }
   }

   public DataSourceInfo getInitalDsInfo() {
      return initialDsInfo;
   }

   
   public String getUri(){
      return this.uri;
   }
   
   public Catalog getInitialCatalog(){
      return this.initialCatalog;
   }
   
   

}
