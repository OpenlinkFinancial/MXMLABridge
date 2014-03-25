/*
 * Copyright (c) 2008-2014 Open Link Financial, Inc. All Rights Reserved.
 */

package custom.mondrian.xmla.exception;

import mondrian.olap.MondrianException;

/**
 * An exception thrown when authentication failed.
 */

public class AuthenticateException extends MondrianException {
  

   private static final long serialVersionUID = -2812294881838936095L;
   
   public AuthenticateException(String faultString) {
      super(faultString, null);
   }
   public AuthenticateException(String faultString, Throwable cause) {
      super(faultString, cause);
   }
   
   
   
}
