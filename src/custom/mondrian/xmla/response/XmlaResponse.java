/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2006 Pentaho and others
// Copyright (c) 2008-2013 Open Link Financial, Inc. All Rights Reserved.
*/
package custom.mondrian.xmla.response;

import custom.mondrian.xmla.writer.SaxWriter;

/**
 * XML/A response interface.
 *
 * @author Gang Chen
 */
public interface XmlaResponse {

    /**
     * Report XML/A error (not SOAP fault).
     */
    public void error(Throwable t);

    /**
     * Get helper for writing XML document.
     */
    public SaxWriter getWriter();
}

// End XmlaResponse.java
