package org.pentaho.di.trans.dataservice.api;

import javax.ws.rs.core.Response;

public interface IDataservicesEndpoint {

  Response queryDataservice( String query );

}
