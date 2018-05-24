package org.pentaho.di.trans.dataservice.endpoint;

import org.pentaho.det.api.data.access.query.IQuery;
import org.pentaho.di.core.sql.SQL;
import org.pentaho.di.trans.dataservice.DataServiceExecutor;
import org.pentaho.di.trans.dataservice.api.IDataservicesPushEndpoint;
import org.pentaho.di.trans.dataservice.clients.DataServiceClient;
import org.pentaho.di.trans.dataservice.resolvers.DataServiceResolverDelegate;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.StreamingOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

@Path( "/dataservices" )
public class DataServicePushEndpoint implements IDataservicesPushEndpoint {

  private DataServiceResolverDelegate resolver;

  public DataServicePushEndpoint( DataServiceResolverDelegate resolver) {
    this.resolver = resolver;
  }

  @POST
  @Path( "/queryDataservicePush/{query}" )
  @Consumes( MediaType.APPLICATION_JSON )
  @Produces( MediaType.APPLICATION_JSON )
  @Override
  public StreamingOutput queryDataservicePush( @PathParam( "query" ) String query,
                                               String windowMode,
                                               long windowSize,
                                               long windowEvery,
                                               long windowMax ) {
    return new StreamingOutput() {
      public void write( final OutputStream out ) throws IOException, WebApplicationException {
        try {
          SQL sql = new SQL( query );

          DataServiceExecutor.Builder builder = resolver.createBuilder( sql );
          builder.streamingType( DataServiceClient.StreamingType.PUSH );
          DataServiceExecutor dataServiceExecutor = builder.build();
          DataOutputStream dataOutputStream = new DataOutputStream( out );
          dataServiceExecutor.executeQuery( dataOutputStream );
        } catch ( Exception e ) { //TODO review exceptions
          throw new RuntimeException( e );
        }
      }
    };
  }
}
