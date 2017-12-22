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

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by bryan on 12/10/15.
 */
class DefaultTransWiringRowAdapter extends RowAdapter {
  public static final String PASSING_ALONG_ROW = "Passing along row: ";
  public static final String ROW_BUFFER_IS_FULL_TRYING_AGAIN = "Row buffer is full, trying again";
  private final Trans serviceTrans;
  private final Trans genTrans;
  private final RowProducer rowProducer;

  private int windowSize = 30;
  private List<RowMetaAndData> rowMetaAndData = Lists.newArrayList();
  private boolean genTransBusy = false;

  public DefaultTransWiringRowAdapter( Trans serviceTrans, Trans genTrans, RowProducer rowProducer ) {
    this.serviceTrans = serviceTrans;
    this.genTrans = genTrans;
    this.rowProducer = rowProducer;
  }

  private synchronized void setGenTransBusy( boolean busy ) {
    this.genTransBusy = busy;
  }

  /**
   * Creates a new thread to process a window of data to the genTrans.
   *
   * @param List<RowMetaAndData> window - The window of rows to be written to the rowProducer.
   */
  public void writeRowsWindowToProducer( List<RowMetaAndData> window ) {
    Thread thread = new Thread(){
      public void run(){
        if( startGenTrans() ) {
          for (int i = 0; i < window.size(); i++) {
            addRowToRowProducer( window.get( i ) );
          }
          rowProducer.finished();
        }
      }
    };
    thread.start();
  }

  /**
   * Starts or Takes over the genTrans if not Started/Taken.
   *
   * @return True if the genTrans is started/taken, false otherwise.
   */
  private boolean startGenTrans() {
    if ( !genTransBusy ) {
      setGenTransBusy( true );
      try {
        if ( this.genTrans.isFinished() ) {
          this.genTrans.startThreads();
        }
        return true;
      } catch ( KettleException e ) {
        setGenTransBusy( false );
        throw Throwables.propagate( e );
      }
    }
    return false;
  }

  /**
   * Adds a row to the genTrans rowProducer step.
   *
   * @param RowMetaAndData row - The row to be added to the producer.
   */
  private void addRowToRowProducer( RowMetaAndData row ) {
    LogChannelInterface log = serviceTrans.getLogChannel();

    while ( !rowProducer.putRowWait ( row.getRowMeta(),
            row.getData(),
            1,
            TimeUnit.SECONDS ) && genTrans.isRunning() ) {
      // Row queue was full, try again
      if ( log.isRowLevel() ) {
        log.logRowlevel(ROW_BUFFER_IS_FULL_TRYING_AGAIN);
      }
    }
  }

  /**
   * Processes a received row from the Service Transformation. If the windowSize is -1 it means that
   * the buffer is disabled, so the row is directely writen to the genTrans rowProducer. Otherwise, adds the row to the
   * buffer and activates the genTrans when the buffer has windowSize elements and the genTrans is not taken,
   * injecting the window records to the rowProducer.
   *
   * @param RowMetaAndData row - The row to be processed.
   */
  private void processRow( RowMetaAndData row ) {
    if ( this.windowSize == -1 ) {
      startGenTrans();
      addRowToRowProducer( row );
    } else {
      this.rowMetaAndData.add( row );
      if ( this.rowMetaAndData.size() >= this.windowSize && ( !genTransBusy ) ) {
        writeRowsWindowToProducer( this.rowMetaAndData.subList( 0, this.windowSize - 1 ) );
      }

      if ( rowMetaAndData.size() > this.windowSize ) {
        rowMetaAndData.removeAll( this.rowMetaAndData.subList( 0, this.rowMetaAndData.size() - this.windowSize ) );
      }
    }
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

      processRow( rowMD );
    } catch ( KettleValueException e ) {
      throw new KettleStepException( e );
    }
  }
}
