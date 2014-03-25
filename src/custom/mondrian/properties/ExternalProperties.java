/*
 * Copyright (c) 2008-2014 Open Link Financial, Inc. All Rights Reserved.
 */

package custom.mondrian.properties;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Properties;
import mondrian.xmla.XmlaException;
import org.apache.log4j.Logger;

/**
 * This class defines additional ads-specific configuration properties that's not 
 * defined in original mondrian.olap.MondrianProperties file.
 * 
 * Configuration properties that determine the behavior of a mondrian instance.
 *
 * <p>There is a method for property valid in a
 * <code>mondrian.properties</code> file. Although it is possible to retrieve
 * properties using the inherited {@link java.util.Properties#getProperty(String)}
 * method, we recommend that you use methods in this class.</p>
*/
public class ExternalProperties {
   
   private static final Logger LOGGER = Logger.getLogger(ExternalProperties.class);

   
   
   /**
    * List of available configurations
    */
  public enum PropName{
      
      DISABLE_MEASURES_CACHING("custom.mondrian.rolap.star.disableMeasuresCaching",
               "Option to disable mondrian caching on cube's measures",
               "boolean",
                false),
      
      DISABLE_SCHEMA_CACHING("custom.mondrian.rolap.schema.disableSchemaCaching",
               "Option to disable mondrian caching on schema meta data",
               "boolean",
                true);
     //Define more ADS-specific mondrian properties here.
      
      String name;
      String description;
      String type;
      Object value;
      
      PropName(String name, String description, String type, Object value){
         this.name = name;
         this.description = description;
         this.type = type;
         this.value = value;
      }
      
      /**
       * Retrieve the value of this enum
       * @return object
       */
      public Object getValue() {
         return value;
      }
      public void setValue( Object value){
         this.value = value;
         
      }
   }

   protected static final String CustomMondrianProperties =  System.getProperty("ads_olap_root") +"/custom.mondrian.properties";
   protected static final String MondrianProperties =  System.getProperty("ads_olap_root") +"/mondrian.properties";

   private  static ExternalProperties instance;
   
   Properties adsProps = new Properties();

   private ExternalProperties() {

   }
   
   
   public static ExternalProperties  getInstance() {
      if (instance == null){
         instance = new ExternalProperties();
         try {
        	instance.populateMondrianProps();
            instance.populateCustomMondrianProps();
         } catch (IOException e) {
            throw new XmlaException("0", "0", "Fail to load custom.mondrian.properties", e);
         }
      }
      return instance;
   }
   
   
   /**
    * Load mondrian configuration into System.Properties from mondrian.properties file.
    * @throws IOException 
    */
      public void populateMondrianProps() throws IOException {
         String path = MondrianProperties;
         File file = new File(path);
         Properties properties = System.getProperties();
         properties.load(new FileInputStream(file));
        
      }  
   
   
/**
 * Fetch the ads-customized mondrian properties from 'custom.mondrian.properties' configuration.
 * @throws IOException 
 */
   public void populateCustomMondrianProps() throws IOException {
      String path = CustomMondrianProperties;
      File file = new File(path);
      FileInputStream input = new FileInputStream(file);
      adsProps.load(input);
   }
   
   
   public boolean isDisableMeasuresCashing(){
      if(adsProps.containsKey(PropName.DISABLE_MEASURES_CACHING.name)){
         return  Boolean.valueOf(adsProps.get(PropName.DISABLE_MEASURES_CACHING.name).toString());
      }
     LOGGER.warn("Property '"+PropName.DISABLE_MEASURES_CACHING+"' is not defined in custom.mondirna.properties"); 
     throw new NoSuchElementException("Property '"+PropName.DISABLE_MEASURES_CACHING+"' is not defined in mondirna.properties");    
   }
   
   public boolean isDisableSchemaCaching(){
      if(adsProps.containsKey(PropName.DISABLE_SCHEMA_CACHING.name)){
         return Boolean.valueOf(adsProps.get(PropName.DISABLE_SCHEMA_CACHING.name).toString());
      }
     LOGGER.warn("Property '"+PropName.DISABLE_SCHEMA_CACHING+"' is not defined in custom.mondirna.properties"); 
     throw new NoSuchElementException("Property '"+PropName.DISABLE_SCHEMA_CACHING+"' is not defined in mondirna.properties");    
   }
   

}
