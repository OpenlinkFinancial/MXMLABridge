/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2010 Pentaho
// All Rights Reserved.
*/
package custom.mondrian.xmla.response.impl;

import mondrian.olap.MondrianException;
import mondrian.olap.Util;
import custom.mondrian.xmla.handler.Enumeration;
import custom.mondrian.xmla.handler.XmlaUtil;
import custom.mondrian.xmla.response.XmlaResponse;
import custom.mondrian.xmla.writer.DefaultSaxWriter;
import custom.mondrian.xmla.writer.SaxWriter;

import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

import org.apache.log4j.Logger;

/**
 * Default implementation of {@link mondrian.xmla.XmlaResponse}.
 *
 * @author Gang Chen
 */
public class DefaultXmlaResponse implements XmlaResponse  {

   private static final Logger LOGGER =
            Logger.getLogger(DefaultXmlaResponse.class);
   
    // TODO: add a msg to MondrianResource for this.
    private static final String MSG_ENCODING_ERROR = "Encoding unsupported: ";

    private  SaxWriter writer;
    private String currentCube;
    
    public DefaultXmlaResponse(
        OutputStream outputStream,
        String encoding,
        Enumeration.ResponseMimeType responseMimeType)
    {
       initWriter(encoding, responseMimeType, outputStream);
    }
    
    private void initWriter( String encoding, Enumeration.ResponseMimeType responseMimeType, OutputStream outputStream) {
       try {
          switch (responseMimeType) {
          case JSON:
             writer = null;
             LOGGER.error("Can not support JSON message, please use SOAP instead");
             throw new MondrianException("Can not support JSON message, please use SOAP instead");
          case SOAP:
          default:
              writer = new DefaultSaxWriter(outputStream, encoding);
              break;
          }
      } catch (UnsupportedEncodingException uee) {
          throw Util.newError(uee, MSG_ENCODING_ERROR + encoding);
      }
    }

    public SaxWriter getWriter() {
        return writer;
    }

    public void error(Throwable t) {
        writer.completeBeforeElement("root");
        @SuppressWarnings({})
        Throwable throwable = XmlaUtil.rootThrowable(t);
        writer.startElement("Messages");
        writer.startElement(
            "Error",
            "ErrorCode", throwable.getClass().getName(),
            "Description", throwable.getMessage(),
            "Source", "Mondrian",
            "Help", "");
        writer.endElement(); // </Messages>
        writer.endElement(); // </Error>
    }
    
    public void setCurrentCube(String currentCube){
       this.currentCube = currentCube;
    }
    
    public String getCurrentCube() {
       return this.currentCube;
    }
}

// End DefaultXmlaResponse.java
