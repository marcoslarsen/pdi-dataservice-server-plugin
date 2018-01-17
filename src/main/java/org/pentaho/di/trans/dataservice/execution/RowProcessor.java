/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2018 by Hitachi Vantara : http://www.pentaho.com
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
import org.pentaho.di.core.exception.KettleTransException;
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
import java.util.concurrent.atomic.AtomicBoolean;

public class RowProcessor {
    public static final String ROW_BUFFER_IS_FULL_TRYING_AGAIN = "Row buffer is full, trying again";

    // Defeult -1 - no limit
    private int windowSize = -1;

    // Time limit in milisecconds default -1 - no limit
    private int timeLimit = -1;

    private List<RowMetaAndData> serviceRowMetaAndDataBuffer;

    private List<RowMetaAndData> serviceOutRowMetaAndDataBuffer;

    private Trans serviceTrans;

    private RowProducer rowProducer;

    private String resultStepName;

    private StepInterface resultStep;

    private Trans genTrans;

    private ManagementMode mode;

    final AtomicBoolean genTransBusy = new AtomicBoolean( false );

    final AtomicBoolean rowMetaWritten = new AtomicBoolean( false );

    // The management mode of the service transformation record buffer
    public enum ManagementMode {
        SIZE,
        TIME,
        ADAPTIVE
    }

    public RowProcessor( Trans serviceTrans, Trans genTrans, RowProducer rowProducer, String resultStepName ) {
        this.mode = ManagementMode.SIZE;
        this.rowProducer = rowProducer;
        this.serviceTrans = serviceTrans;
        this.genTrans = genTrans;
        this.serviceRowMetaAndDataBuffer =  Lists.newArrayList();
        this.resultStepName = resultStepName;
        this.serviceOutRowMetaAndDataBuffer =  Lists.newArrayList();
    }

    public RowProcessor(ManagementMode mode, Trans serviceTrans, Trans genTrans, RowProducer rowProducer, StepInterface resultStep ) {
        this.mode = mode;
        this.rowProducer = rowProducer;
        this.serviceTrans = serviceTrans;
        this.genTrans = genTrans;
        this.serviceRowMetaAndDataBuffer =  Lists.newArrayList();
        this.resultStep = resultStep;
        this.serviceOutRowMetaAndDataBuffer =  Lists.newArrayList();
    }

    public int getWindowSize() {
        return windowSize;
    }

    public void setWindowSize( int windowSize ) {
        this.windowSize = windowSize;
    }

    public int getTimeLimit() {
        return timeLimit;
    }

    public void setTimeLimit( int timeLimit ) {
        this.timeLimit = timeLimit;
    }

    public ManagementMode getMode() {
        return mode;
    }

    public void setMode( ManagementMode mode ) {
        this.mode = mode;
    }

    /**
     * Processes a received row from the Service Transformation. If the windowSize is -1 it means that
     * the buffer is disabled, so the row is directly written to the genTrans rowProducer. Otherwise, adds the row to
     * the buffer and activates the genTrans when the buffer has windowSize elements and the genTrans is not taken,
     * injecting the window records to the rowProducer.
     *
     * @param RowMetaAndData row - The row to be processed.
     */
    public void processRow( RowMetaAndData row ) {
        if ( this.windowSize == -1 ) {
            startGenTrans( this.genTrans );
            addRowToRowProducer( row );
        } else {
            this.serviceRowMetaAndDataBuffer.add( row );
            if ( this.serviceRowMetaAndDataBuffer.size() >= this.windowSize && ( !this.genTransBusy.get() ) ) {
                writeRowsWindowToProducer( new ArrayList( this.serviceRowMetaAndDataBuffer.subList( 0, this.windowSize - 1 ) ) );
            }

            if ( this.serviceRowMetaAndDataBuffer.size() > this.windowSize ) {
                this.serviceRowMetaAndDataBuffer.removeAll(
                        this.serviceRowMetaAndDataBuffer.subList( 0,
                                this.serviceRowMetaAndDataBuffer.size() - this.windowSize ) );
            }
        }
    }

    /**
     * Creates a new thread to process a window of data to the genTrans.
     *
     * @param List<RowMetaAndData> window - The window of rows to be written to the rowProducer.
     */
    private void writeRowsWindowToProducer(List<RowMetaAndData> window ) {
        this.genTransBusy.compareAndSet( false, true );
        Thread thread = new Thread(){
            public void run(){
          if( startGenTrans( genTrans) ) {
              for (int i = 0; i < window.size(); i++) {
                  addRowToRowProducer(window.get(i));
              }
              rowProducer.finished();
              genTrans.waitUntilFinished();
             // genTrans.stopAll();
              genTransBusy.compareAndSet(true, false);
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
    private boolean startGenTrans( Trans genTrans) {
        if ( this.genTrans.isFinishedOrStopped() ) {
            try {
                this.genTrans.startThreads();
                this.resultStep = this.genTrans.findRunThread( this.resultStepName );
                this.resultStep.addRowListener( getResultRowAdapter() );
                return true;
            } catch ( KettleException e ) {
                return false;
            }
        } else if ( this.genTrans.isReadyToStart() ) {
            this.resultStep = this.genTrans.findRunThread( this.resultStepName );
            this.resultStep.addRowListener( getResultRowAdapter() );
            return true;
        }
        return false;
    }

    private RowAdapter getResultRowAdapter() {
       return new RowAdapter() {
            @Override
            public void rowWrittenEvent(RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {
                try {
                    Object[] rowData = rowMeta.cloneRow( row );
                    RowMetaAndData rowMD = new RowMetaAndData( rowMeta, rowData );

                    serviceOutRowMetaAndDataBuffer.add( rowMD );

                    if ( serviceOutRowMetaAndDataBuffer.size() > windowSize ) {
                        serviceOutRowMetaAndDataBuffer.removeAll(
                                serviceOutRowMetaAndDataBuffer.subList( 0,
                                        serviceOutRowMetaAndDataBuffer.size() - windowSize ) );
                    }

                } catch ( KettleValueException e ) {
                    throw new KettleStepException( e );
                }
            }
        };
    }

    /**
     * Adds a row to the genTrans rowProducer step.
     *
     * @param RowMetaAndData row - The row to be added to the producer.
     */
    private void addRowToRowProducer( RowMetaAndData row ) {
        LogChannelInterface log = this.serviceTrans.getLogChannel();

        while ( !this.rowProducer.putRowWait ( row.getRowMeta(),
                row.getData(),
                1,
                TimeUnit.SECONDS ) && this.genTrans.isRunning() ) {
            // Row queue was full, try again
            if ( log.isRowLevel() ) {
                log.logRowlevel(ROW_BUFFER_IS_FULL_TRYING_AGAIN);
            }
        }
    }
}
