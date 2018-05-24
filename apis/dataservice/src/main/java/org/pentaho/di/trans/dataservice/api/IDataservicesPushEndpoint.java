package org.pentaho.di.trans.dataservice.api;

import javax.ws.rs.core.StreamingOutput;

public interface IDataservicesPushEndpoint {

  StreamingOutput queryDataservicePush( String query, String windowMode, long windowSize, long windowEvery, long windowMax );

}
