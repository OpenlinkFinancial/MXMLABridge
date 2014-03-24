/*
 * Copyright (c) 2008-2013 Open Link Financial, Inc. All Rights Reserved.
 */

package custom.mondrian.xmla.dataSource;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import mondrian.xmla.XmlaException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * Processor generate valid datasource.xml with configurations in olap.xml
 */

public class DataSourceProcessor {

   /*
    * Process replaces following tags with proper value Validation failed if any
    * of them not found.
    */
   private static final String INITIAL_DATASOURCE_IDENTIFIER = "${initial_datasource}";
   private static final String URL_IDENTIFIER = "${url}";
   private static final String CATALOGS_IDENTIFIER = "${catalogs}";
   protected static final String DSINFO = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> \n <DataSources>"
            + "<DataSource> \n <DataSourceName>Provider=Mondrian;DataSource=MondrianFoodMart;</DataSourceName> \n"
            + "<DataSourceDescription>Mondrian FoodMart Data Warehouse</DataSourceDescription> \n <URL>" +URL_IDENTIFIER+"</URL> \n" + INITIAL_DATASOURCE_IDENTIFIER + "\n"
            + "<ProviderName>Mondrian</ProviderName> \n" + "<ProviderType>MDP</ProviderType> \n"
            + "<AuthenticationMode>Unauthenticated</AuthenticationMode> \n" +  CATALOGS_IDENTIFIER + "\n</DataSource></DataSources>";

   /** Serialized olap.xml file */
   private String olapConfig;

   
   /** The full path to catalog folder*/
   private String catalogRoot;
   
   /* e.g.:  user:[user];password:[password] */
   private String credential;
   private DataSource dataSource;

   private static DataSourceProcessor instance;
   
   public DataSourceProcessor(){
      
   }
   private DataSourceProcessor(String olapConfig) {
      this.olapConfig = olapConfig;
   }
   
   /**
    * 
    * @param olapConfig
    * @param dsConfig
    * @return
    */
   public static DataSourceProcessor instance(String olapConfig){
      if (instance == null)
            instance = new DataSourceProcessor(olapConfig);
      return instance;
   }

   public String process() {

      this.dataSource = unMarshalDataSourceXml();      
      return marshalDataSourceXml(this.dataSource);
   }

   /**
    * Unmarshal the serialized DSINFO with olap.xml to entitiy objects: Catalogs, Catalog,
    * DatasourceInfo
    * 
    * @return data source
    * 
    */
   public DataSource unMarshalDataSourceXml() throws custom.mondrian.xmla.exception.XmlaException {
      try {
         DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
         InputSource is = new InputSource();
         is.setCharacterStream(new StringReader(olapConfig));
         Document doc = db.parse(is);
         Element olapElement = doc.getDocumentElement();
         
         // Elements under <olap>/<server>
         Element serverElement = (Element) olapElement.getElementsByTagName("server").item(0);
         String host = serverElement.getElementsByTagName("host").item(0).getTextContent();
         String port = serverElement.getElementsByTagName("port").item(0).getTextContent();
         String context = serverElement.getElementsByTagName("web-context").item(0).getTextContent();

         String uri = "http://" + host + ":" + port + context;
         String relativeCatalogPath = olapElement.getElementsByTagName("schemas-folder").item(0).getTextContent();
         catalogRoot = System.getProperty("ads_olap_root") + "/xmla" + relativeCatalogPath; 
         Element jdbcElement = (Element) olapElement.getElementsByTagName("jdbc").item(0);
         String jdbcUri = jdbcElement.getElementsByTagName("uri").item(0).getTextContent();
         
         //insert login credential to jdbcUri
         if(credential != null)
            jdbcUri = jdbcUri +"?" + credential;
         
         String jdbcDriver = jdbcElement.getElementsByTagName("driver").item(0).getTextContent();

         Catalogs catalogs = new Catalogs();
         for (String catalogName : getCatalogs(catalogRoot)) {
            DataSourceInfo ds = new DataSourceInfo(jdbcUri, jdbcDriver);
            Catalog catalog = new Catalog(catalogName, relativeCatalogPath, ds);
            catalogs.addCatalog(catalog);
         }

         // populate DataSource
         DataSource dataSource = new DataSource(catalogs, uri);
         return dataSource;

      } catch (ParserConfigurationException | SAXException | IOException e) {
         // TODO Auto-generated catch block
         throw new XmlaException("0", "0", "Fail the unmarshal datasource xml string ", null);
      }

   }

   /**
    * Update dsXml string with DataSource entity
    * 
    * @param dsXml
    * @param ds
    * @return
    */
   public String marshalDataSourceXml(DataSource ds) {

      String dsXml = DSINFO;
      // validate the input datasource.xml in valid format.
      if (!dsXml.contains(CATALOGS_IDENTIFIER) || !dsXml.contains(INITIAL_DATASOURCE_IDENTIFIER) || !dsXml.contains(URL_IDENTIFIER)) {

         throw new XmlaException("0", "0", "The input datasource.xml is in invalid format", null);
      }

      dsXml = dsXml.replace(CATALOGS_IDENTIFIER, ds.getCatalogs().getElementXmlString());
      dsXml = dsXml.replace(INITIAL_DATASOURCE_IDENTIFIER, ds.getInitalDsInfo().getElementXmlString());
      dsXml = dsXml.replace(URL_IDENTIFIER, ds.getUri());

      return dsXml;
   }

   /**
    * Return List of catalog names within the catalog directory
    * 
    * @return List<String> catalog files
    * @throws FileNotFoundException
    */
   public List<String> getCatalogs(String catalogDir) throws FileNotFoundException {

      List<String> result = new ArrayList<String>();
      if (catalogDir == null || catalogDir == "")
         throw new FileNotFoundException("Catalog directory not exist");

      File dir = new File(catalogDir);

      if (!dir.isDirectory())
         throw new FileNotFoundException("Catalog directory not exist");

      File[] catalogs = dir.listFiles();

      for (File catalog : catalogs) {
         if (catalog.isFile() && catalog.getName().endsWith(".xml"))
            result.add(catalog.getName());
      }

      if (result.size() == 0)
         throw new FileNotFoundException("Not calalogs found");

      return result;
   }
   public String getCredential() {
      return credential;
   }
   public void setCredential(String credential) {
      this.credential = credential;
   }

   
   

}
