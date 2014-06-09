/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2012 Pentaho
// Copyright (c) 2008-2013 Open Link Financial, Inc. All Rights Reserved.
*/
package custom.mondrian.xmla.servlet.impl;

import mondrian.olap.CacheControl;
import mondrian.olap.MondrianServer;
import mondrian.rolap.RolapConnection;
import mondrian.spi.CatalogLocator;
import mondrian.spi.impl.ServletContextCatalogLocator;
import custom.mondrian.xmla.exception.XmlaException;
import custom.mondrian.xmla.dataSource.CustomUrlRepositoryContentFinder;
import custom.mondrian.xmla.handler.Enumeration;
import custom.mondrian.xmla.handler.PropertyDefinition;
import custom.mondrian.xmla.handler.CustomXmlaHandler;
import custom.mondrian.xmla.request.XmlaRequestCallback;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.w3c.dom.Element;



/**
 * Extension to {@link mondrian.xmla.XmlaServlet} that instantiates a Mondrian
 * engine.
 * 
 */
public class MondrianXmlaServlet extends DefaultXmlaServlet {

   private static final long serialVersionUID = -4383762697678904164L;
   
   protected MondrianServer server;
   protected CatalogLocator catalogLocator;
   protected CustomUrlRepositoryContentFinder contentFinder;
   protected ServletConfig servletConfig;
   public static String initialCatalog = "";
   

   @Override
   protected CustomXmlaHandler.ConnectionFactory createConnectionFactory(ServletConfig servletConfig) throws ServletException {
      this.servletConfig = servletConfig;
      if (server == null) {
         // A derived class can alter how the calalog locator object is
         // created.
         catalogLocator = makeCatalogLocator(servletConfig);

         // fetch the default schema name
         if (contentFinder == null) {
            contentFinder = makeContentFinder();
         }
         // setInitialCatalog(dataSources);
         PropertyDefinition.setCatalogValue(contentFinder.getInitialCatalog());

         server = MondrianServer.createWithRepository(contentFinder, catalogLocator);

      }
      return (CustomXmlaHandler.ConnectionFactory) server;
   }
   

   /**
    * Main entry for HTTP post method
    * 
    */

   @Override
   protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

      /*
       * This makes Mondrian supports for multiple data sources, which are
       * identified by URI e.g.: "http:\\localhost\xmla\ads\foodmart" for data
       * source foodmart "http:\\localhost\xmla\ads\cashflow" for data source
       * cashflow
       * 
       * 
       * Servelt stores currently running data source name in dataSource field.
       * Servelt updates the value of currentDataSource, if the data source of
       * new request changed
       */
      // validate if the datasource of request changed
      currentUri = request.getRequestURI();
      this.userPrincipal = request.getUserPrincipal();
      contentFinder = makeContentFinder();
      PropertyDefinition.setCatalogValue(contentFinder.getInitialCatalog());

     /**
      * rquestSoapParts[0]: header
      * rquestSoapParts[0]: body
      */
      Element[] requestSoapParts = new Element[2];

      /**
       * responseSoapParts[0]: header
       * responseSoapParts[0]: body
       */
      byte[][] responseSoapParts = new byte[2][];

      Phase phase = Phase.VALIDATE_HTTP_HEAD;
     
      /**
       * This bridge supports only SOAP message
       */
      final Enumeration.ResponseMimeType mimeType = Enumeration.ResponseMimeType.SOAP;

      try {
            try {
               request.setCharacterEncoding(charEncoding);
               response.setCharacterEncoding(charEncoding);
            } catch (UnsupportedEncodingException uee) {
               charEncoding = null;
               LOGGER.warn("Unsupported character encoding '" + charEncoding + "': Use default character encoding from HTTP client " + "for now");
            }

         response.setContentType(mimeType.getMimeType());

         Map<String, Object> context = new HashMap<String, Object>();

         try {
            if (LOGGER.isDebugEnabled()) {
               LOGGER.debug("Invoking validate http header callbacks");
            }
            for (XmlaRequestCallback callback : getCallbacks()) {
               if (!callback.processHttpHeader(request, response, context)) {
                  return;
               }
            }
         } catch (XmlaException xex) {
            LOGGER.error("Errors when invoking callbacks validateHttpHeader", xex);
            handleFault(response, responseSoapParts, phase, xex);
            phase = Phase.SEND_ERROR;
            marshallSoapMessage(response, responseSoapParts, mimeType);
            return;
         } catch (Exception ex) {
            LOGGER.error("Errors when invoking callbacks validateHttpHeader", ex);
            handleFault(response, responseSoapParts, phase, new XmlaException(SERVER_FAULT_FC, CHH_CODE, CHH_FAULT_FS, ex));
            phase = Phase.SEND_ERROR;
            marshallSoapMessage(response, responseSoapParts, mimeType);
            return;
         }

         phase = Phase.INITIAL_PARSE;

         try {
            if (LOGGER.isDebugEnabled()) {
               LOGGER.debug("Unmarshalling SOAP message");
            }

            // check request's content type
            String contentType = request.getContentType();
            if (contentType == null || !contentType.contains("text/xml")) {
               throw new IllegalArgumentException("Only accepts content type 'text/xml', not '" + contentType + "'");
            }


            context.put(CONTEXT_MIME_TYPE, mimeType);
            unmarshallSoapMessage(request, requestSoapParts);
         } catch (XmlaException xex) {
            LOGGER.error("Unable to unmarshall SOAP message", xex);
            handleFault(response, responseSoapParts, phase, xex);
            phase = Phase.SEND_ERROR;
            marshallSoapMessage(response, responseSoapParts, mimeType);
            return;
         }

         
         phase = Phase.PROCESS_HEADER;

         try {
            if (LOGGER.isDebugEnabled()) {
               LOGGER.debug("Handling XML/A message header");
            }

            // process application specified SOAP header here
            handleSoapHeader(response, requestSoapParts, responseSoapParts, context);
         } catch (XmlaException xex) {
            LOGGER.error("Errors when handling XML/A message", xex);
            handleFault(response, responseSoapParts, phase, xex);
            phase = Phase.SEND_ERROR;
            marshallSoapMessage(response, responseSoapParts, mimeType);
            return;
         }

         
         phase = Phase.CALLBACK_PRE_ACTION;

         try {
            if (LOGGER.isDebugEnabled()) {
               LOGGER.debug("Invoking callbacks preAction");
            }

            for (XmlaRequestCallback callback : getCallbacks()) {
               callback.preAction(request, requestSoapParts, context);
            }
         } catch (XmlaException xex) {
            LOGGER.error("Errors when invoking callbacks preaction", xex);
            handleFault(response, responseSoapParts, phase, xex);
            phase = Phase.SEND_ERROR;
            marshallSoapMessage(response, responseSoapParts, mimeType);
            return;
         } catch (Exception ex) {
            LOGGER.error("Errors when invoking callbacks preaction", ex);
            handleFault(response, responseSoapParts, phase, new XmlaException(SERVER_FAULT_FC, CPREA_CODE, CPREA_FAULT_FS, ex));
            phase = Phase.SEND_ERROR;
            marshallSoapMessage(response, responseSoapParts, mimeType);
            return;
         }
         
         

         phase = Phase.PROCESS_BODY;

         try {
            if (LOGGER.isDebugEnabled()) {
               LOGGER.debug("Handling XML/A message body");
            }

            handleSoapBody(response, requestSoapParts, responseSoapParts, context);
         } catch (XmlaException xex) {
            LOGGER.error("Errors when handling XML/A message", xex);
            handleFault(response, responseSoapParts, phase, xex);
            phase = Phase.SEND_ERROR;
            marshallSoapMessage(response, responseSoapParts, mimeType);
            return;
         }


         phase = Phase.CALLBACK_POST_ACTION;

         try {
            if (LOGGER.isDebugEnabled()) {
               LOGGER.debug("Invoking callbacks postAction");
            }

            for (XmlaRequestCallback callback : getCallbacks()) {
               callback.postAction(request, response, responseSoapParts, context);
            }
         } catch (XmlaException xex) {
            LOGGER.error("Errors when invoking callbacks postaction", xex);
            handleFault(response, responseSoapParts, phase, xex);
            phase = Phase.SEND_ERROR;
            marshallSoapMessage(response, responseSoapParts, mimeType);
            return;
         } catch (Exception ex) {
            LOGGER.error("Errors when invoking callbacks postaction", ex);
            handleFault(response, responseSoapParts, phase, new XmlaException(SERVER_FAULT_FC, CPOSTA_CODE, CPOSTA_FAULT_FS, ex));
            phase = Phase.SEND_ERROR;
            marshallSoapMessage(response, responseSoapParts, mimeType);
            return;
         }

         phase = Phase.SEND_RESPONSE;

         try {
            response.setStatus(HttpServletResponse.SC_OK);
            marshallSoapMessage(response, responseSoapParts, mimeType);
         } catch (XmlaException xex) {
            LOGGER.error("Errors when handling XML/A message", xex);
            handleFault(response, responseSoapParts, phase, xex);
            phase = Phase.SEND_ERROR;
            marshallSoapMessage(response, responseSoapParts, mimeType);
         }
      } catch (Throwable t) {
         LOGGER.error("Unknown Error when handling XML/A message", t);
         handleFault(response, responseSoapParts, phase, t);
         marshallSoapMessage(response, responseSoapParts, mimeType);
      }
   }

   @Override
   public void destroy() {
      super.destroy();
      if (server != null) {
         server.shutdown();
         server = null;
      }
   }

   /**
    * @return RepositoryContentFinder used to load data source configuration file
    */
   protected CustomUrlRepositoryContentFinder makeContentFinder() {
      if(this.userPrincipal != null)
         return new CustomUrlRepositoryContentFinder(userPrincipal.getName());
      return new CustomUrlRepositoryContentFinder();
   }

   /**
    * Make catalog locator. Derived classes can roll their own.
    * 
    * @param servletConfig
    *           Servlet configuration info
    * @return Catalog locator
    */
   protected CatalogLocator makeCatalogLocator(ServletConfig servletConfig) {
      ServletContext servletContext = servletConfig.getServletContext();
      return new ServletContextCatalogLocator(servletContext);
   }

 
   
   public CacheControl getCacheControl(RolapConnection connection, PrintWriter pw){
      return server.getAggregationManager().getCacheControl(connection, pw);
   }
}

// End MondrianXmlaServlet.java
