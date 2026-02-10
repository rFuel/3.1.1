      PROMPT ""
$INCLUDE I_Prologue
$IFNDEF isRT
      FNAMES = ""
      DIM FHANDLES(1000)
      MAT FHANDLES = ""
$ENDIF
      GOOD=""
      FOR SS = 32 TO 126
         GOOD := CHAR(SS)
      NEXT SS
      LOG.KEY = "dLOADER":@FM
      LOG.MSG = "----------------------------------------------------"
      CALL uLOGGER(0, LOG.KEY:LOG.MSG)
      LOG.MSG = "Starting DATA.LOADER"
      CALL uLOGGER(0, LOG.KEY:LOG.MSG)
      LOG.MSG = "----------------------------------------------------"
      CALL uLOGGER(0, LOG.KEY:LOG.MSG)
      atIM = "<im>"
      atFM = "<fm>"
      atVM = "<vm>"
      atSM = "<sv>"
      DOT  = "_"
      UPL  = "upl_"
      RFUEL=1
      DELTA=0
      OPEN "INBOUND.DELTAS" TO INBOUND    ELSE STOP "No INBOUND.DELTAS file"
      OPEN "uDELTA.LOG"     TO DELTA.LOAD ELSE DELTA=0
      OPEN "VOC"            TO VOC        ELSE STOP "Cannot open VOC"
      OPEN "BP.UPL" TO BP.UPL ELSE STOP "No BP.UPL file"
      *
      EXE = "SSELECT INBOUND.DELTAS"
      LOOP
         READ STOP.SW FROM BP.UPL, "STOP" ELSE STOP.SW=""
         IF INDEX(UPCASE(STOP.SW), "STOP", 1) THEN 
            LOG.MSG = "****************************************************"
            CALL uLOGGER(0, LOG.KEY:LOG.MSG)
            LOG.MSG = "Stopping DATA.LOADER"
            CALL uLOGGER(0, LOG.KEY:LOG.MSG)
            LOG.MSG = "****************************************************"
            CALL uLOGGER(0, LOG.KEY:LOG.MSG)
            STOP
         END
         EXECUTE EXE CAPTURING JUNK
         LOOP
            READNEXT LOGID ELSE EXIT
            READU REC FROM INBOUND, LOGID LOCKED CONTINUE ELSE CONTINUE
            DELETE INBOUND, LOGID
            RELEASE INBOUND, LOGID
            INREC = EREPLACE(REC, atIM, @FM)
            ACCT  = INREC<1>
            FILE  = INREC<2>
            ITEM  = INREC<3>
            RECORD= INREC<4>
            * RECORD = "f1<fm>f2<fm>f3.1<vm>f3.2 .... etc."
            RECORD = EREPLACE(RECORD, atFM, @FM)
            RECORD = EREPLACE(RECORD, atVM, @VM)
            RECORD = EREPLACE(RECORD, atSM, @SM)
            * RECORD = is now a true pick record            
            HANDLE = UPL:ACCT:DOT:FILE
            LOCATE(HANDLE, FNAMES; FPOS) ELSE
               VREC = "Q":@FM:ACCT:@FM:FILE
               WERR = 0
$IFDEF isRT
               WRITE VREC ON VOC, HANDLE ON ERROR WERR = 1
$ELSE
               WRITE VREC ON VOC, HANDLE ELSE WERR = 1
$ENDIF
               IF WERR THEN
                  CRT "ERROR: cannot write ":HANDLE:" to VOC"
                  RELEASE
$IFNDEF isRT
                  CLOSE
$ENDIF
                  STOP
               END
               OPEN HANDLE TO IOFILE THEN
                  FNAMES<-1> = HANDLE
                  LOCATE(HANDLE, FNAMES; FPOS) ELSE
                     CRT "ERROR: did not add ":HANDLE:" into FNAMES"
                     RELEASE
$IFNDEF isRT
                     CLOSE
$ENDIF
                     STOP
                  END
                  FHANDLES(FPOS) = IOFILE
               END ELSE
                  LOG.MSG = LOGID:" Cannot Open / Create ":HANDLE
                  CALL uLOGGER(0, LOG.KEY:LOG.MSG)
                  CONTINUE
               END
            END
            IOFILE = FHANDLES(FPOS)
            READU BASE.REC FROM IOFILE, ITEM ELSE BASE.REC = ""
            IF BASE.REC # RECORD THEN
               WERR = 0
$IFDEF isRT
               WRITE RECORD ON IOFILE, ITEM ON ERROR WERR = 1
$ELSE
               WRITE RECORD ON IOFILE, ITEM ELSE WERR = 1
$ENDIF
               IF WERR THEN
                  CRT "ERROR: write error on ":HANDLE:" for item ":ITEM
               END
               RELEASE IOFILE, ITEM
               LOG.MSG = LOGID:" loaded into ":FNAMES<FPOS>
               CALL uLOGGER(0, LOG.KEY:LOG.MSG)
            END ELSE
               RELEASE IOFILE, ITEM
            END
            * ----------------------------------------------------------
            * RFUEL flag: Write the update to uDELTA.LOG for rFuel:022  
            *           : Check *.LOADED first to be sure it hasn't     
            *             been sent previously. Kafka logs will repeat  
            *           : If not sent, write to uDELTA.LOG and move on  
            * ----------------------------------------------------------
            IF RFUEL THEN
               LFILE = FILE:"_":ACCT:".LOADED"
               LOCATE(LFILE, FNAMES; LPOS) THEN
                  LOADED = FHANDLES(LPOS)
               END ELSE
                  OKAY=0; TRIES=1; ERR=""
                  LOOP UNTIL OKAY DO
                     OPEN LFILE TO LOADED THEN
                        OKAY = 1
                     END ELSE
                        IF TRIES => 2 THEN ERR=1; OKAY=1; CONTINUE
                        CF = "CREATE.FILE ":LFILE:" 30"
                        EXECUTE CF CAPTURING JUNK
                        TRIES += 1
                     END
                  REPEAT
                  IF ERR # "" THEN
                     LOG.MSG = LOGID:"ABORT: OPEN-CREATE failed on ":LFILE
                     CALL uLOGGER(0, LOG.KEY:LOG.MSG)
                     STOP
                  END
                  FNAMES<-1> = LFILE
                  LOCATE(LFILE, FNAMES; LPOS) THEN
                     FHANDLES(LPOS) = LOADED
                  END ELSE
                     LOG.MSG = LOGID:"ABORT: FNAMES-FHANDLE failure on ":LFILE
                     CALL uLOGGER(0, LOG.KEY:LOG.MSG)
                     STOP
                  END
               END
               *
               CALL uMD5 (THIS.HASH, RECORD)
               READU PREV.HASH FROM LOADED, ITEM ELSE PREV.HASH=""
               IF PREV.HASH # THIS.HASH THEN
                  WRITE THIS.HASH ON LOADED, ITEM
                  IF DELTA THEN
                     * ----------------------------------------------------
                     * RECORD cannot have control chars - it is going into 
                     * a SQL database - which will throw a fit !           
                     CALL SCRUB.DATA(GOOD, RECORD)
                     * ----------------------------------------------------
                     dREC = EREPLACE(RECORD, @FM , atFM)
                     dREC = EREPLACE(dREC,   @VM , atVM)
                     dREC = EREPLACE(dREC,   @SM , atSM)
                     rfKEY = ACCT:atIM:FILE:atIM:ITEM
                     WRITE dREC ON DELTA.LOAD, rfKEY
                     LOG.MSG = LOGID:" sent to uStreams "
                     CALL uLOGGER(0, LOG.KEY:LOG.MSG)
                     LOG.MSG = LOGID:"---- "
                     CALL uLOGGER(0, LOG.KEY:LOG.MSG)
                  END
               END
               RELEASE LOADED, ITEM
            END
         REPEAT
***      NAP 500
         RQM ; RQM
      REPEAT
      CRT "Finishing at ":TIMEDATE()
      CRT
      RELEASE
$IFNDEF isRT
      CLOSE
$ENDIF
      STOP
   END
