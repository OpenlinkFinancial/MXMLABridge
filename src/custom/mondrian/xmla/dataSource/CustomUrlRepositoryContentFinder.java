/*
 * Copyright (c) 2008-2013 Open Link Financial, Inc. All Rights Reserved.
 */

package custom.mondrian.xmla.dataSource;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import org.apache.log4j.Logger;
import mondrian.server.RepositoryContentFinder;

import mondrian.xmla.XmlaException;

/**
 * Class used to load data source configurations. 
 * 
 */

public class CustomUrlRepositoryContentFinder implements RepositoryContentFinder {
   
   protected static Logger LOGGER = Logger.getLogger(CustomUrlRepositoryContentFinder.class);
   
   private DataSourceProcessor dataSourceProcessor;
   protected String initialCatalog;
   protected DataSource dataSource;
   protected String credential;
  
   /**
    * Creates a UrlRepositoryContentFinder.
    * 
    * @param url
    *           URL of repository
    */
   public CustomUrlRepositoryContentFinder() {
      init();
   }
   
   public CustomUrlRepositoryContentFinder(String credential){
      this.credential = credential;
      init();
      
   }
   
   private void init(){
      try {
         
      // load olap configuration file olap.xml
      String olapRoot = System.getProperty("ads_olap_root");
      String olapConfigFullPath = olapRoot + "/olap.xml";

      File olapFile = new File(olapConfigFullPath);
      if (!olapFile.exists() || !olapFile.isFile()) {
         throw new XmlaException("0", "0", "Can not find olap configuration file: olap.xml", null);
      }
      BufferedReader br = new BufferedReader(new FileReader(olapFile));
      StringBuilder olapXmlStr = new StringBuilder();
      String line;
      while ((line = br.readLine()) != null) {
         olapXmlStr.append(line);
      }

      this.dataSourceProcessor= DataSourceProcessor.instance(olapXmlStr.toString());
      if(credential != null)
         dataSourceProcessor.setCredential(credential);
      this.dataSource = dataSourceProcessor.unMarshalDataSourceXml();
      this.initialCatalog = dataSource.getInitialCatalog().getName();
      
      } catch (IOException e) {
         throw new XmlaException("0", "0", "Can not find olap configuration file: olap.xml", null);
      }
   }

   public String getContent() {
         String dsStr = dataSourceProcessor.process();
         return dsStr;

   }
   
   public DataSource getCurrentDataSource() {
      return this.dataSource;
   }

   public String getInitialCatalog() {
      return this.initialCatalog;
   }

   public void shutdown() {
      // nothing to do
   }

}
