/*
 * Copyright (c) 2008-2014 Open Link Financial, Inc. All Rights Reserved.
 */

package custom.mondrian.xmla.handler;

import custom.mondrian.xmla.request.XmlaRequest;
import custom.mondrian.xmla.response.XmlaResponse;

/**
 * <class description goes here>
 * 
 */

public abstract class Processor {
   
   XmlaRequest request;
   XmlaResponse response;
   CustomXmlaHandler handler;
   
   public Processor(CustomXmlaHandler handler, XmlaRequest request, XmlaResponse response){
      this.request = request;
      this.response = response;
      this.handler = handler;
      
   }
   
   abstract void contextMeta();
   abstract void rowsetXmlaSchema();
   abstract void data();
}


class discoverProcessor extends Processor {
   RowsetDefinition rowSetDef;
   Rowset rowSet;

   public discoverProcessor(CustomXmlaHandler handler, XmlaRequest request, XmlaResponse response) {
      super(handler, request, response);
      rowSetDef = RowsetDefinition.valueOf(request.getRequestType());  
      rowSet = rowSetDef.getRowset(request, handler);
      }

   @Override
   void contextMeta() {
      // TODO Auto-generated method stub
      
   }

   @Override
   void rowsetXmlaSchema() {
      // TODO Auto-generated method stub
      
   }

   @Override
   void data() {
      // TODO Auto-generated method stub
      
   }
   
}

class executeProcessor extends Processor {

   public executeProcessor(CustomXmlaHandler handler, XmlaRequest request, XmlaResponse response) {
      super(handler, request, response);
      // TODO Auto-generated constructor stub
   }

   @Override
   void contextMeta() {
      // TODO Auto-generated method stub
      
   }

   @Override
   void rowsetXmlaSchema() {
      // TODO Auto-generated method stub
      
   }

   @Override
   void data() {
      // TODO Auto-generated method stub
      
   }
   
}
