/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2011 Pentaho
// Copyright (c) 2008-2013 Open Link Financial, Inc. All Rights Reserved.
*/
package custom.mondrian.xmla.servlet;

import org.apache.log4j.Logger;

import org.w3c.dom.Element;

import custom.mondrian.properties.ExternalProperties;
import custom.mondrian.xmla.exception.XmlaException;
import custom.mondrian.xmla.handler.Enumeration;
import custom.mondrian.xmla.handler.XmlaConstants;
import custom.mondrian.xmla.handler.CustomXmlaHandler;
import custom.mondrian.xmla.request.XmlaRequestCallback;
import java.util.*;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.*;

/**
 * Base XML/A servlet.
 *
 * @author Gang Chen
 * @since December, 2005
 */
public abstract class XmlaServlet
    extends HttpServlet
    implements XmlaConstants
{

   private static final long serialVersionUID = -2085761517812820752L;

   protected static Logger LOGGER = Logger.getLogger(XmlaServlet.class);

    public static final String PARAM_DATASOURCES_CONFIG = "DataSourcesConfig";
    public static final String PARAM_OPTIONAL_DATASOURCE_CONFIG =
        "OptionalDataSourceConfig";
    public static final String PARAM_CHAR_ENCODING = "CharacterEncoding";
    public static final String PARAM_CALLBACKS = "Callbacks";

    protected CustomXmlaHandler xmlaHandler = null;
    protected String charEncoding = null;
    private final List<XmlaRequestCallback> callbackList =
        new ArrayList<XmlaRequestCallback>();

    private CustomXmlaHandler.ConnectionFactory connectionFactory;
    public  boolean isDiscoverRowSet = false;

    public enum Phase {
        VALIDATE_HTTP_HEAD,
        INITIAL_PARSE,
        CALLBACK_PRE_ACTION,
        PROCESS_HEADER,
        PROCESS_BODY,
        CALLBACK_POST_ACTION,
        SEND_RESPONSE,
        SEND_ERROR
    }

    /**
     * Returns true if paramName's value is not null and 'true'.
     */
    public static boolean getBooleanInitParameter(
        ServletConfig servletConfig,
        String paramName)
    {
        String paramValue = servletConfig.getInitParameter(paramName);
        return paramValue != null && Boolean.valueOf(paramValue);
    }

    public static boolean getParameter(
        HttpServletRequest req,
        String paramName)
    {
        String paramValue = req.getParameter(paramName);
        return paramValue != null && Boolean.valueOf(paramValue);
    }

    public XmlaServlet() {
    }


    /**
     * Initializes servlet and XML/A handler.
     *
     */
    public void init(ServletConfig servletConfig)
        throws ServletException
    {
        super.init(servletConfig);
        //init: mondrian configurations
        ExternalProperties.getInstance();
        
        // init: charEncoding
        initCharEncodingHandler(servletConfig);

        // init: callbacks
        initCallbacks(servletConfig);

        // init: connection factory
        // MondrianServerImpl implements ConnectionFactory interface
        this.connectionFactory = createConnectionFactory(servletConfig);
    }

    protected abstract CustomXmlaHandler.ConnectionFactory createConnectionFactory(
        ServletConfig servletConfig)
        throws ServletException;

    /**
     * Gets (creating if needed) the XmlaHandler.
     *
     * @return XMLA handler
     */
    protected CustomXmlaHandler getXmlaHandler() {
        if (this.xmlaHandler == null) {
            this.xmlaHandler =
                new CustomXmlaHandler(
                    connectionFactory,
                    "cxmla");
        }
        return this.xmlaHandler;
    }

    /**
     * Registers a callback.
     */
    protected final void addCallback(XmlaRequestCallback callback) {
        callbackList.add(callback);
    }

    /**
     * Returns the list of callbacks. The list is immutable.
     *
     * @return list of callbacks
     */
    protected final List<XmlaRequestCallback> getCallbacks() {
        return Collections.unmodifiableList(callbackList);
    }



    /**
     * Implement to provide application specified SOAP unmarshalling algorithm.
     */
    protected abstract void unmarshallSoapMessage(
        HttpServletRequest request,
        Element[] requestSoapParts)
        throws XmlaException;

    /**
     * Implement to handle application specified SOAP header.
     */
    protected abstract void handleSoapHeader(
        HttpServletResponse response,
        Element[] requestSoapParts,
        byte[][] responseSoapParts,
        Map<String, Object> context)
        throws XmlaException;

    /**
     * Implement to handle XML/A request.
     */
    protected abstract void handleSoapBody(
        HttpServletResponse response,
        Element[] requestSoapParts,
        byte[][] responseSoapParts,
        Map<String, Object> context)
        throws XmlaException;

    /**
     * Implement to provide application specified SOAP marshalling algorithm.
     */
    protected abstract void marshallSoapMessage(
        HttpServletResponse response,
        byte[][] responseSoapParts,
        Enumeration.ResponseMimeType responseMimeType)
        throws XmlaException;

    /**
     * Implement to application specified handler of SOAP fualt.
     */
    protected abstract void handleFault(
        HttpServletResponse response,
        byte[][] responseSoapParts,
        Phase phase,
        Throwable t);

    /**
     * Initialize character encoding
     */
    protected void initCharEncodingHandler(ServletConfig servletConfig) {
        String paramValue = servletConfig.getInitParameter(PARAM_CHAR_ENCODING);
        if (paramValue != null) {
            this.charEncoding = paramValue;
        } else {
            this.charEncoding = "UTF-8";
            LOGGER.warn("Use default character encoding from HTTP client");
        }
    }

    /**
     * Registers callbacks configured in web.xml.
     * 
     */
    protected void initCallbacks(ServletConfig servletConfig) {
        String callbacksValue = servletConfig.getInitParameter(PARAM_CALLBACKS);

        if (callbacksValue != null) {
            String[] classNames = callbacksValue.split(";");

            int count = 0;
            nextCallback:
            for (String className1 : classNames) {
                String className = className1.trim();

                try {
                    Class<?> cls = Class.forName(className);
                    if (XmlaRequestCallback.class.isAssignableFrom(cls)) {
                        XmlaRequestCallback callback =
                            (XmlaRequestCallback) cls.newInstance();

                        try {
                            callback.init(servletConfig);
                        } catch (Exception e) {
                            LOGGER.warn(
                                "Failed to initialize callback '"
                                + className + "'",
                                e);
                            continue nextCallback;
                        }

                        addCallback(callback);
                        count++;

                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(
                                "Register callback '" + className + "'");
                        }
                    } else {
                        LOGGER.warn(
                            "'" + className + "' is not an implementation of '"
                            + XmlaRequestCallback.class + "'");
                    }
                } catch (ClassNotFoundException cnfe) {
                    LOGGER.warn(
                        "Callback class '" + className + "' not found",
                        cnfe);
                } catch (InstantiationException ie) {
                    LOGGER.warn(
                        "Can't instantiate class '" + className + "'",
                        ie);
                } catch (IllegalAccessException iae) {
                    LOGGER.warn(
                        "Can't instantiate class '" + className + "'",
                        iae);
                }
            }
            LOGGER.debug(
                "Registered " + count + " callback" + (count > 1 ? "s" : ""));
        }
    }
}

// End XmlaServlet.java
