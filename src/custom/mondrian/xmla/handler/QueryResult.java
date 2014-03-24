/*
 * Copyright (c) 2008-2013 Open Link Financial, Inc. All Rights Reserved.
 */

package custom.mondrian.xmla.handler;

import java.sql.SQLException;

import org.olap4j.OlapException;
import org.xml.sax.SAXException;


import custom.mondrian.xmla.writer.SaxWriter;

/**
 * <class description goes here>
 * 
 */

 abstract class QueryResult {
   abstract void unparse(SaxWriter res) throws SAXException, OlapException;

   abstract void close() throws SQLException;

   abstract void metadata(SaxWriter writer);
}
 

  