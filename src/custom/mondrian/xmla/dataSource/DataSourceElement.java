/*
 * Copyright (c) 2008-2013 Open Link Financial, Inc. All Rights Reserved.
 */

package custom.mondrian.xmla.dataSource;

/**
 * All entity classes of data source configuration extends this class
 */

public abstract class  DataSourceElement {
   
   
   /**
    * Serialize the xml element into String format.
    * @return
    */
   public abstract String getElementXmlString();
      
}
