/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.dataservice.execution;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.StepInterface;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by bryan on 12/10/15.
 */
class DefaultTransWiringRowAdapter extends RowAdapter {
  public static final String PASSING_ALONG_ROW = "Passing along row: ";
  private final Trans serviceTrans;
  private final Trans genTrans;
  private final RowProducer rowProducer;
  private final RowProcessor rowProcessor;

  public DefaultTransWiringRowAdapter( Trans serviceTrans, Trans genTrans, RowProducer rowProducer, String resultStepName ) {
    this.serviceTrans = serviceTrans;
    this.genTrans = genTrans;
    this.rowProducer = rowProducer;
    this.rowProcessor = new RowProcessor( serviceTrans, genTrans, rowProducer, resultStepName );

    this.rowProcessor.setWindowSize( 30 );
  }

  @Override
  public void rowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {
    // Simply pass along the row to the other transformation (to the Injector step)
    //
    LogChannelInterface log = serviceTrans.getLogChannel();
    try {
      if ( log.isRowLevel() ) {
        log.logRowlevel( PASSING_ALONG_ROW + rowMeta.getString( row ) );
      }
    } catch ( KettleValueException e ) {
      // Ignore errors
    }

    try {
      Object[] rowData = rowMeta.cloneRow( row );
      RowMetaAndData rowMD = new RowMetaAndData( rowMeta, rowData );

      this.rowProcessor.processRow( rowMD );

    } catch ( KettleValueException e ) {
      throw new KettleStepException( e );
    }
  }
}
