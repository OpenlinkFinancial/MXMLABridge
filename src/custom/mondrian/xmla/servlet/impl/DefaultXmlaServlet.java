/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2011 Pentaho
// Copyright (c) 2008-2013 Open Link Financial, Inc. All Rights Reserved.
 */
package custom.mondrian.xmla.servlet.impl;

import custom.mondrian.xmla.exception.AuthenticateException;
import custom.mondrian.xmla.exception.XmlaException;
import custom.mondrian.xmla.handler.Enumeration;
import custom.mondrian.xmla.handler.XmlaConstants;
import custom.mondrian.xmla.handler.XmlaUtil;
import custom.mondrian.xmla.handler.Enumeration.ResponseMimeType;
import custom.mondrian.xmla.request.XmlaRequest;
import custom.mondrian.xmla.request.XmlaRequestCallback;
import custom.mondrian.xmla.request.impl.DefaultXmlaRequest;
import custom.mondrian.xmla.response.XmlaResponse;
import custom.mondrian.xmla.response.impl.DefaultXmlaResponse;
import custom.mondrian.xmla.servlet.XmlaServlet;
import custom.mondrian.xmla.writer.DefaultSaxWriter;
import custom.mondrian.xmla.writer.SaxWriter;

import mondrian.olap.MondrianException;
import org.w3c.dom.*;
import org.xml.sax.SAXException;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.parsers.*;

/**
 * Default implementation of XML/A servlet.
 */
public abstract class DefaultXmlaServlet extends XmlaServlet {


   private static final long serialVersionUID = 4347542470502737032L;

   protected static final String nl = System.getProperty("line.separator");
   private static final String ANONYMOUS_USER_NAME = "anonymous";
   /**
    * Servlet config parameter that determines whether the xmla servlet requires
    * authenticated sessions.
    */
   private static final String REQUIRE_AUTHENTICATED_SESSIONS = "requireAuthenticatedSessions";

   protected DocumentBuilderFactory domFactory = null;

   private boolean requireAuthenticatedSessions = false;
   protected String currentUri;
   protected Principal userPrincipal;
   protected String username;
   protected String password;
   protected String roleName = "user";

   /**
    * Session properties, keyed by session ID. Currently just username and
    * password.
    */
   private final Map<String, SessionInfo> sessionInfos = new HashMap<String, SessionInfo>();

   public void init(ServletConfig servletConfig) throws ServletException {
      this.domFactory = getDocumentBuilderFactory();
      super.init(servletConfig);
      this.requireAuthenticatedSessions = Boolean.parseBoolean(servletConfig.getInitParameter(REQUIRE_AUTHENTICATED_SESSIONS));

   }

   protected static DocumentBuilderFactory getDocumentBuilderFactory() {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      factory.setIgnoringComments(true);
      factory.setIgnoringElementContentWhitespace(true);
      factory.setNamespaceAware(true);
      return factory;
   }

   /**
    * Populate requestSoapParts with request 
    */
   protected void unmarshallSoapMessage(HttpServletRequest request, Element[] requestSoapParts) throws XmlaException {
      try {
         InputStream inputStream;
         InputStream tmpStream;
         
         DocumentBuilder domBuilder;
         Document soapDoc = null;

         
         //init inputStream
         try {
            inputStream = request.getInputStream();
            tmpStream = request.getInputStream();
         } catch (IllegalStateException ex) {
            throw new XmlaException(SERVER_FAULT_FC, USM_REQUEST_STATE_CODE, USM_REQUEST_STATE_FAULT_FS, ex);
         } catch (IOException ex) {
            // This is either Client or Server
            throw new XmlaException(SERVER_FAULT_FC, USM_REQUEST_INPUT_CODE, USM_REQUEST_INPUT_FAULT_FS, ex);
         }

         //init domBuilder
         try { 
            domBuilder = domFactory.newDocumentBuilder();
         } catch (ParserConfigurationException ex) {
            throw new XmlaException(SERVER_FAULT_FC, USM_DOM_FACTORY_CODE, USM_DOM_FACTORY_FAULT_FS, ex);
         }

         // init soapDoc
         try {
            
            //clone inputstream to byteArrayOutputStream 
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int len;
            while ((len = inputStream.read(buffer)) > -1) {
               stream.write(buffer, 0, len);
            }
            stream.flush();
            
            InputStream modified = new ByteArrayInputStream(stream.toByteArray());
            InputStream original = new ByteArrayInputStream(stream.toByteArray());
          
            BufferedReader reader = new BufferedReader(new InputStreamReader(modified, "UTF8"));
            String tmp1Str = reader.readLine();
            StringBuilder modifiedReqStr = new StringBuilder();

            if (tmp1Str != null && !tmp1Str.startsWith("<Envelope") && !tmp1Str.startsWith("<")) {
               tmp1Str = tmp1Str.split("<Envelope")[1];
               modifiedReqStr.append("<Envelope ");
            

            while (tmp1Str != null) {
               modifiedReqStr.append(tmp1Str);
               System.out.println(tmp1Str);
               tmp1Str = reader.readLine();
               modifiedReqStr.append("\n");
            }
            System.out.println(modifiedReqStr.toString());
            InputStream outputStream = new ByteArrayInputStream(modifiedReqStr.toString().getBytes(StandardCharsets.UTF_8));

            soapDoc = domBuilder.parse(outputStream);
            }
            else
               soapDoc = domBuilder.parse(original);
         } catch (IOException ex) {
            // This is either Client or Server
            throw new XmlaException(SERVER_FAULT_FC, USM_DOM_PARSE_IO_CODE, USM_DOM_PARSE_IO_FAULT_FS, ex);
         } catch (SAXException ex) {
             throw new XmlaException(CLIENT_FAULT_FC, USM_DOM_PARSE_CODE,
             USM_DOM_PARSE_FAULT_FS, ex);
         }

         /* Validate incoming XMLA request */
         Element envElem = soapDoc.getDocumentElement();

         if (LOGGER.isDebugEnabled()) {
            logXmlaRequest(envElem);
         }

         if ("Envelope".equals(envElem.getLocalName())) {
            if (!(NS_SOAP_ENV_1_1.equals(envElem.getNamespaceURI()))) {
               throw new XmlaException(CLIENT_FAULT_FC, USM_DOM_PARSE_CODE, USM_DOM_PARSE_FAULT_FS, new SAXException("Invalid SOAP message: "
                        + "Envelope element not in SOAP namespace"));
            }
         } else {
            throw new XmlaException(CLIENT_FAULT_FC, USM_DOM_PARSE_CODE, USM_DOM_PARSE_FAULT_FS, new SAXException("Invalid SOAP message: " + "Top element not Envelope"));
         }

         /* Get request Header */
         Element[] childs = XmlaUtil.filterChildElements(envElem, NS_SOAP_ENV_1_1, "Header");
         if (childs.length > 1) {
            throw new XmlaException(CLIENT_FAULT_FC, USM_DOM_PARSE_CODE, USM_DOM_PARSE_FAULT_FS, new SAXException("Invalid SOAP message: " + "More than one Header elements"));
         }
         requestSoapParts[0] = childs.length == 1 ? childs[0] : null;

         /* Get request Body */
         childs = XmlaUtil.filterChildElements(envElem, NS_SOAP_ENV_1_1, "Body");
         if (childs.length != 1) {
            throw new XmlaException(CLIENT_FAULT_FC, USM_DOM_PARSE_CODE, USM_DOM_PARSE_FAULT_FS, new SAXException("Invalid SOAP message: " + "Does not have one Body element"));
         }
         requestSoapParts[1] = childs[0];
         
      } catch (XmlaException xex) {
         throw xex;
      } catch (Exception ex) {
         throw new XmlaException(SERVER_FAULT_FC, USM_UNKNOWN_CODE, USM_UNKNOWN_FAULT_FS, ex);
      }
   }

   /**
    * Log incoming XMLA request
    * @param envElem
    */
   protected void logXmlaRequest(Element envElem) {
      final StringWriter writer = new StringWriter();
      writer.write("XML/A request content");
      writer.write(nl);
      XmlaUtil.element2Text(envElem, writer);
      if(LOGGER.isDebugEnabled())
         LOGGER.debug(writer.toString());
   }

   /**
    * {@inheritDoc}
    * 
    *Header contains information about:
    * Session
    * Authentication
    * Security
    */
   protected void handleSoapHeader(HttpServletResponse response, Element[] requestSoapParts, byte[][] responseSoapParts, Map<String, Object> context) throws XmlaException {
      try {
         Element hdrElem = requestSoapParts[0];
         if ((hdrElem == null) || (!hdrElem.hasChildNodes())) {
            return;
         }

         String encoding = response.getCharacterEncoding();

         byte[] bytes = null;

         NodeList nlst = hdrElem.getChildNodes();
         int nlen = nlst.getLength();
         boolean authenticatedSession = (this.userPrincipal != null);
         boolean beginSession = false;
         for (int i = 0; i < nlen; i++) {
            Node n = nlst.item(i);
            if (!(n instanceof Element)) {
               continue;
            }
            Element e = (Element) n;
            String localName = e.getLocalName();

            
            /* Security */
            
            if (localName.equals(XMLA_SECURITY) && NS_SOAP_SECEXT.equals(e.getNamespaceURI())) {
               // Example:
               //
               // <Security
               // xmlns="http://schemas.xmlsoap.org/ws/2002/04/secext">
               // <UsernameToken>
               // <Username>MICHELE</Username>
               // <Password Type="PasswordText">ROSSI</Password>
               // </UsernameToken>
               // </Security>
               // <BeginSession mustUnderstand="1"
               // xmlns="urn:schemas-microsoft-com:xml-analysis" />
               
               NodeList childNodes = e.getChildNodes();
               Element userNameToken = (Element) childNodes.item(1);
               NodeList userNamePassword = userNameToken.getChildNodes();
               Element username = (Element) userNamePassword.item(1);
               Element password = (Element) userNamePassword.item(3);
               String userNameStr = username.getChildNodes().item(0).getNodeValue();
               context.put(CONTEXT_XMLA_USERNAME, userNameStr);
               String passwordStr = "";

               if (password.getChildNodes().item(0) != null) {
                  passwordStr = password.getChildNodes().item(0).getNodeValue();
               }

               context.put(CONTEXT_XMLA_PASSWORD, passwordStr);

               if ("".equals(passwordStr) || null == passwordStr) {
                  LOGGER.warn("Security header for user [" + userNameStr + "] provided without password");
               }
               authenticatedSession = true;
               continue;
            }

            // Is it an XMLA element
            if (!NS_XMLA.equals(e.getNamespaceURI())) {
               continue;
            }
            
            
            /* Session */
            
            // So, an XMLA mustUnderstand-er
            // Do we know what to do with it
            // We understand:
            // BeginSession
            // Session
            // EndSession

            String sessionIdStr;
            if (localName.equals(XMLA_BEGIN_SESSION)) {
               sessionIdStr = generateSessionId(context);

               context.put(CONTEXT_XMLA_SESSION_ID, sessionIdStr);
               context.put(CONTEXT_XMLA_SESSION_STATE, CONTEXT_XMLA_SESSION_STATE_BEGIN);

            } else if (localName.equals(XMLA_SESSION)) {
               sessionIdStr = getSessionIdFromRequest(e, context);

               SessionInfo sessionInfo = getSessionInfo(sessionIdStr);
               
               if (sessionInfo != null) {
                  context.put(CONTEXT_XMLA_USERNAME, sessionInfo.user);
                  context.put(CONTEXT_XMLA_PASSWORD, sessionInfo.password);
               }

               else {
                  context.put(CONTEXT_XMLA_USERNAME, ANONYMOUS_USER_NAME);
                  context.put(CONTEXT_XMLA_PASSWORD, "");
               }

               context.put(CONTEXT_XMLA_SESSION_ID, sessionIdStr);
               context.put(CONTEXT_XMLA_SESSION_STATE, CONTEXT_XMLA_SESSION_STATE_WITHIN);

            } else if (localName.equals(XMLA_END_SESSION)) {
               sessionIdStr = getSessionIdFromRequest(e, context);
               context.put(CONTEXT_XMLA_SESSION_STATE, CONTEXT_XMLA_SESSION_STATE_END);

            } else {
               // error
               String msg = "Invalid XML/A message: Unknown " + "\"mustUnderstand\" XMLA Header element \"" + localName + "\"";
               throw new XmlaException(MUST_UNDERSTAND_FAULT_FC, HSH_MUST_UNDERSTAND_CODE, HSH_MUST_UNDERSTAND_FAULT_FS, new RuntimeException(msg));
            }

            StringBuilder buf = new StringBuilder(100);
            buf.append("<Session ");
            buf.append(XMLA_SESSION_ID);
            buf.append("=\"");
            buf.append(sessionIdStr);
            buf.append("\" ");
            buf.append("xmlns=\"");
            buf.append(NS_XMLA);
            buf.append("\" />");
            bytes = buf.toString().getBytes(encoding);

            
            /* Authentication */
            if (authenticatedSession) {
            
               saveSessionInfo(this.userPrincipal.getName(), sessionIdStr);
               if(LOGGER.isDebugEnabled())
                   LOGGER.debug("New authenticated session; storing credentials [" + this.username + "/********] for session id [" + sessionIdStr + "]");

            } else {
               if (beginSession && requireAuthenticatedSessions) {
                  throw new XmlaException(XmlaConstants.CLIENT_FAULT_FC, XmlaConstants.CHH_AUTHORIZATION_CODE, XmlaConstants.CHH_AUTHORIZATION_FAULT_FS, new Exception(
                           "Session Credentials NOT PROVIDED"));
               }
            }
         }
         responseSoapParts[0] = bytes;
      } catch (XmlaException xex) {
         throw xex;
      } catch (Exception ex) {
         throw new XmlaException(SERVER_FAULT_FC, HSH_UNKNOWN_CODE, HSH_UNKNOWN_FAULT_FS, ex);
      }
   }

   protected String generateSessionId(Map<String, Object> context) {
      for (XmlaRequestCallback callback : getCallbacks()) {
         final String sessionId = callback.generateSessionId(context);
         if (sessionId != null) {
            return sessionId;
         }
      }

      // Generate a pseudo-random new session ID.
      return Long.toString(17L * System.nanoTime() + 3L * System.currentTimeMillis(), 35);
   }

   private static String getSessionIdFromRequest(Element e, Map<String, Object> context) throws Exception {
      // extract the SessionId attrs value and put into context
      Attr attr = e.getAttributeNode(XMLA_SESSION_ID);
      if (attr == null) {
         throw new SAXException("Invalid XML/A message: " + XMLA_SESSION + " Header element with no " + XMLA_SESSION_ID + " attribute");
      }

      String sessionId = attr.getValue();
      if (sessionId == null) {
         throw new SAXException("Invalid XML/A message: " + XMLA_SESSION + " Header element with " + XMLA_SESSION_ID + " attribute but no attribute value");
      }
      return sessionId;
   }

   protected void handleSoapBody(HttpServletResponse response, Element[] requestSoapParts, byte[][] responseSoapParts, Map<String, Object> context) throws XmlaException {
      try {
         String encoding = response.getCharacterEncoding();
         Element bodyElem = requestSoapParts[1];
         
         //It's either a "Discover" reqeust or "Execute" request
         Element[] dreqs = XmlaUtil.filterChildElements(bodyElem, NS_XMLA, "Discover");
         Element[] ereqs = XmlaUtil.filterChildElements(bodyElem, NS_XMLA, "Execute");
         if (dreqs.length + ereqs.length != 1) {
            throw new XmlaException(CLIENT_FAULT_FC, HSB_BAD_SOAP_BODY_CODE, HSB_BAD_SOAP_BODY_FAULT_FS, new RuntimeException("Invalid XML/A message: Body has " + dreqs.length
                     + " Discover Requests and " + ereqs.length + " Execute Requests"));
         }

         Element xmlaReqElem = (dreqs.length == 0 ? ereqs[0] : dreqs[0]);

         ByteArrayOutputStream osBuf = new ByteArrayOutputStream();


         String sessionId = (String) context.get(CONTEXT_XMLA_SESSION_ID);
         XmlaRequest xmlaReq = (XmlaRequest) new DefaultXmlaRequest(xmlaReqElem, roleName, username, password, sessionId);

         Enumeration.ResponseMimeType responseMimeType = Enumeration.ResponseMimeType.SOAP;
         final String responseMimeTypeName = (String) xmlaReq.getProperties().get("ResponseMimeType");
         if (responseMimeTypeName != null) {
            responseMimeType = (ResponseMimeType) Enumeration.ResponseMimeType.MAP.get(responseMimeTypeName);
            if (responseMimeType != null) {
               context.put(CONTEXT_MIME_TYPE, responseMimeType);
            }
         }

         XmlaResponse xmlaRes = new DefaultXmlaResponse(osBuf, encoding, responseMimeType);

         /* Use XmlaHandler to process XMLA request  */
         try {
            getXmlaHandler().process(xmlaReq, xmlaRes);
         } catch (XmlaException ex) {
            throw ex;
         } catch (Exception ex) {
            throw new XmlaException(SERVER_FAULT_FC, HSB_PROCESS_CODE, HSB_PROCESS_FAULT_FS, ex);
         }

         responseSoapParts[1] = osBuf.toByteArray();
       
      } catch (XmlaException xex) {
         throw xex;
      } catch (Exception ex) {
         throw new XmlaException(SERVER_FAULT_FC, HSB_UNKNOWN_CODE, HSB_UNKNOWN_FAULT_FS, ex);
      }
   }

   protected void marshallSoapMessage(HttpServletResponse response, byte[][] responseSoapParts, Enumeration.ResponseMimeType responseMimeType) throws XmlaException {
      try {
         // If CharacterEncoding was set in web.xml, use this value
         String encoding = (charEncoding != null) ? charEncoding : response.getCharacterEncoding();

         if (charEncoding != null) {
            response.setCharacterEncoding(charEncoding);
         }
         switch (responseMimeType) {
         case JSON:
            throw new MondrianException("Doesn't support Jason message type");
         case SOAP:
         default:
            response.setContentType("text/xml");
            break;
         }

         /*
          * The setCharacterEncoding, setContentType, or setLocale method must
          * be called BEFORE getWriter or getOutputStream and before committing
          * the response for the character encoding to be used.
          * 
          * @see javax.servlet.ServletResponse
          */
         OutputStream outputStream = response.getOutputStream();

         byte[] soapHeader = responseSoapParts[0];
         byte[] soapBody = responseSoapParts[1];

         Object[] byteChunks = null;

         try {
            switch (responseMimeType) {
 
            case SOAP:
            default:
               String s0 = "<?xml version=\"1.0\" encoding=\"" + encoding + "\"?>\n<" + SOAP_PREFIX + ":Envelope xmlns:" + SOAP_PREFIX + "=\"" + NS_SOAP_ENV_1_1 + "\"" + ">"
                        + "\n<" + SOAP_PREFIX + ":Header>\n";
               String s2 = "</" + SOAP_PREFIX + ":Header>\n" + "<" + SOAP_PREFIX + ":Body>\n";
               String s4 = "\n</" + SOAP_PREFIX + ":Body>\n</" + SOAP_PREFIX + ":Envelope>\n";

               byteChunks = new Object[] { s0.getBytes(encoding), soapHeader, s2.getBytes(encoding), soapBody, s4.getBytes(encoding), };
               break;
            }
         } catch (UnsupportedEncodingException uee) {
            LOGGER.warn("This should be handled at begin of processing request", uee);
         }

         if (LOGGER.isDebugEnabled()) {
            StringBuilder buf = new StringBuilder(100);
            buf.append("XML/A response content").append(nl);
            try {
               for (Object byteChunk : byteChunks) {
                  byte[] chunk = (byte[]) byteChunk;
                  if (chunk != null && chunk.length > 0) {
                     buf.append(new String(chunk, encoding));
                  }
               }
            } catch (UnsupportedEncodingException uee) {
               LOGGER.warn("This should be handled at begin of processing request", uee);
            }
            if(LOGGER.isDebugEnabled())
               LOGGER.debug(buf.toString());
         }

         if (LOGGER.isDebugEnabled()) {
            StringBuilder buf = new StringBuilder();
            buf.append("XML/A response content").append(nl);
         }
         try {
            int bufferSize = 4096;
            ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
            WritableByteChannel wch = Channels.newChannel(outputStream);
            ReadableByteChannel rch;
            for (Object byteChunk : byteChunks) {
               if (byteChunk == null || ((byte[]) byteChunk).length == 0) {
                  continue;
               }
               rch = Channels.newChannel(new ByteArrayInputStream((byte[]) byteChunk));

               int readSize;
               do {
                  buffer.clear();
                  readSize = rch.read(buffer);
                  buffer.flip();

                  int writeSize = 0;
                  while ((writeSize += wch.write(buffer)) < readSize) {
                  }
               } while (readSize == bufferSize);
               rch.close();
            }
            outputStream.flush();
         } catch (IOException ioe) {
            LOGGER.error("Damn exception when transferring bytes over sockets", ioe);
         }
      } catch (XmlaException xex) {
         throw xex;
      } catch (Exception ex) {
         throw new XmlaException(SERVER_FAULT_FC, MSM_UNKNOWN_CODE, MSM_UNKNOWN_FAULT_FS, ex);
      }
   }

   /**
    * This produces a SOAP 1.1 version Fault element - not a 1.2 version.
    * 
    */
   
   protected void handleFault(HttpServletResponse response, byte[][] responseSoapParts, Phase phase, Throwable t) {
      // Regardless of whats been put into the response so far, clear
      // it out.
      response.reset();

      // NOTE: if you can think of better/other status codes to use
      // for the various phases, please make changes.
      // I think that XMLA faults always returns OK.
      switch (phase) {
      case VALIDATE_HTTP_HEAD:
         response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
         break;
      case INITIAL_PARSE:
      case CALLBACK_PRE_ACTION:
      case PROCESS_HEADER:
      case PROCESS_BODY:
      case CALLBACK_POST_ACTION:
      case SEND_RESPONSE:
         response.setStatus(HttpServletResponse.SC_OK);
         break;
      }

      String code;
      String faultCode;
      String faultString;
      String detail;
      if (t instanceof XmlaException) {
         XmlaException xex = (XmlaException) t;
         code = xex.getCode();
         faultString = xex.getFaultString();
         faultCode = XmlaException.formatFaultCode(xex);
         detail = XmlaException.formatDetail(xex.getDetail());

      } else {
         // some unexpected Throwable
         t = XmlaException.getRootCause(t);
         code = UNKNOWN_ERROR_CODE;
         faultString = UNKNOWN_ERROR_FAULT_FS;
         faultCode = XmlaException.formatFaultCode(SERVER_FAULT_FC, code);
         detail = XmlaException.formatDetail(t.getMessage());
      }

      String encoding = response.getCharacterEncoding();

      ByteArrayOutputStream osBuf = new ByteArrayOutputStream();
      try {
         SaxWriter writer = new DefaultSaxWriter(osBuf, encoding);
         writer.startDocument();
         writer.startElement(SOAP_PREFIX + ":Fault");

         // The faultcode element is intended for use by software to provide
         // an algorithmic mechanism for identifying the fault. The faultcode
         // MUST be present in a SOAP Fault element and the faultcode value
         // MUST be a qualified name
         writer.startElement("faultcode");
         writer.characters(faultCode);
         writer.endElement();

         // The faultstring element is intended to provide a human readable
         // explanation of the fault and is not intended for algorithmic
         // processing.
         writer.startElement("faultstring");
         writer.characters(faultString);
         writer.endElement();

         // The faultactor element is intended to provide information about
         // who caused the fault to happen within the message path
         writer.startElement("faultactor");
         writer.characters(FAULT_ACTOR);
         writer.endElement();

         // The detail element is intended for carrying application specific
         // error information related to the Body element. It MUST be present
         // if the contents of the Body element could not be successfully
         // processed. It MUST NOT be used to carry information about error
         // information belonging to header entries. Detailed error
         // information belonging to header entries MUST be carried within
         // header entries.
         if (phase != Phase.PROCESS_HEADER) {
            writer.startElement("detail");
            writer.element("Error", "ErrorCode", 0, "Description", detail, "Source","","HelpFile","");
//            writer.startElement(FAULT_NS_PREFIX + ":error", "xmlns:" + FAULT_NS_PREFIX, MONDRIAN_NAMESPACE);
//            writer.startElement("code");
//            writer.characters(code);
//            writer.endElement(); // code
//            writer.startElement("desc");
//            writer.characters(detail);
//            writer.endElement(); // desc
//            writer.endElement(); // error
            writer.endElement(); // detail
         }

         writer.endElement(); // </Fault>
         writer.endDocument();
      } catch (UnsupportedEncodingException uee) {
         LOGGER.warn("This should be handled at begin of processing request", uee);
      } catch (Exception e) {
         LOGGER.error("Unexcepted runimt exception when handing SOAP fault :(");
      }

      responseSoapParts[1] = osBuf.toByteArray();
   }

   private SessionInfo getSessionInfo(String sessionId) throws AuthenticateException {
      if (sessionId == null) {
         return null;
      }

      SessionInfo sessionInfo = null;

      synchronized (sessionInfos) {
         sessionInfo = sessionInfos.get(sessionId);
      }

      if (sessionInfo == null) {
         //pass user principal to session
         if(this.userPrincipal == null){
             if(LOGGER.isDebugEnabled())
                 LOGGER.debug("Use the anonymous credentials for session id [" + sessionId + "], user " + ANONYMOUS_USER_NAME);
        	 return saveSessionInfo(ANONYMOUS_USER_NAME, sessionId);
         }
         return saveSessionInfo(this.userPrincipal.getName(), sessionId);
      } else {
         if(LOGGER.isDebugEnabled())
            LOGGER.debug("Found credentials for session id [" + sessionId + "], username=[" + sessionInfo.user + "] in servlet cache");
      }
      return sessionInfo;
   }

   private SessionInfo saveSessionInfo(String principals, String sessionId) throws AuthenticateException {
      synchronized (sessionInfos) {
         //principals is in the format username: [name]; password: [password]
         SessionInfo sessionInfo = sessionInfos.get(sessionId);

         if(sessionInfo == null && principals.equals(ANONYMOUS_USER_NAME)){
        	 sessionInfo = new SessionInfo(principals, "");
         }
         else {
            if(username == null)
               parsePrincipals(principals);
            // A credentials object was stored against the provided session
            // ID but the username didn't match, so create a new holder.
            sessionInfo = new SessionInfo(username, password);
            sessionInfos.put(sessionId, sessionInfo);
         }
         return sessionInfo;
      }
   }
   
   /**
    * Input principal is serizlized form of username and password
    * It's in the format of "username:[name];password:[password]"
    * @param principal
    * @return
    */
   private void parsePrincipals(String principal) throws AuthenticateException {
     try{
      if(!principal.startsWith("user"))
         throw new XmlaException("0", "0", "The user principal is in invalid format", null);
      
      String userName = principal.split("&#38;password")[0];
      userName = userName.split("user=")[1];
         if (!principal.endsWith("&#38;password=")) {
            String password = principal.split("&#38;password=")[1];
            this.password = password;

         }
      this.username = userName;
     }
     catch (Exception e) {
        throw new AuthenticateException("Failt to process user principle", e);
     }
   }

   /**
    * Holds authentication credentials of a XMLA session.
    */
   private static class SessionInfo {
      final String user;
      final String password;

      public SessionInfo(String user, String password) {
         this.user = user;
         this.password = password;
      }
   }
}
// End DefaultXmlaServlet.java
