      SUBROUTINE SR_DBWRITER_UV (RTN.STRING, MAT REC.ARR, MAT FILE.ARR, MAT ID.ARR, MAT WV.ARR, MAT WU.ARR)
$INCLUDE I_Prologue
$IFDEF isRT
      DIM REC.ARR(100)
      DIM FILE.ARR(100)
      DIM ID.ARR(100) 
      DIM WV.ARR(100) 
      DIM WU.ARR(100) 
$ENDIF
      RTN.STRING = ""
      nbrELEMENTS= 0
      PRECISION 9
      STX = TIME()
      LOG.KEY = MEMORY.VARS(1):@FM
      CALL SR.GET.PROPERTY ("lock.wait", ans)
      IF NUM(ans) AND ans # "" THEN LOCK.WAIT=ans ELSE LOCK.WAIT = 3
      IF LOCK.WAIT > 0 THEN LOCK.WAIT -= 0.5
      *
      * Pass # 1 : Trap simple errors
      *
      FOR I = 1 TO 100
         IF ID.ARR(I) = "" THEN 
            nbrELEMENTS = I - 1
            EXIT
         END
         RECORD = REC.ARR(I)          ;* the record to write
         FILE.H = FILE.ARR(I)         ;* the file to write to
         ID     = ID.ARR(I)           ;* the item id
         WV     = WV.ARR(I)           ;* WRITEV: needs an attribute atr# or empty
         WU     = WU.ARR(I)           ;* WRITEU: keep it locked? 1/0
         
$IFDEF isRT
         FILE.HNAME = FILEPATH(FILE.H)<3>
         NOT.OPEN   = FILE.H = ""
$ELSE
         FILE.HNAME = FILEINFO(FILE.H, 1)
         NOT.OPEN   = FILEINFO(FILE.H, 0) = 0
$ENDIF         
         
         * validation
         
         REC.CHECK = CONVERT(@FM, "", RECORD)
         SKIP.MSG = "Skipping write to ":FILE.HNAME
         BEGIN CASE
            CASE NOT.OPEN
                  RTN.STRING = "412-Precondition Failed: unOPENed file handle on iteration [":I:"] ":SKIP.MSG
                  IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:RTN.STRING)
                  ID.ARR(I) = "" ; * RETURN
            CASE TRIM(ID) = ""
                  RTN.STRING = "412-Precondition Failed: empty item ID on iteration [":I:"] ":SKIP.MSG
                  IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:RTN.STRING)
                  ID.ARR(I) = "" ; * RETURN
            CASE TRIM(RECORD) = ""
                  RTN.STRING = "412-Precondition Failed: NULL record. ":SKIP.MSG
                  ID.ARR(I) = "" ; * RETURN
            CASE TRIM(REC.CHECK) = ""
                  RTN.STRING = "412-Precondition Failed: empty record. ":SKIP.MSG
                  IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:RTN.STRING)
                  ID.ARR(I) = "" ; * RETURN
            CASE WV # ""
               IF NOT(NUM(WV)) THEN
                  RTN.STRING = "412-Precondition Failed: WV Set with bad attribute [":WV:"] ":SKIP.MSG
                  CALL uLOGGER(0, LOG.KEY:RTN.STRING)
                  ID.ARR(I) = "" ; * RETURN
               END
               IF DCOUNT(RECORD, @FM) < WV THEN
                  RTN.STRING = "412-Precondition Failed: WV [":WV:"] is beyond the length of the record. ":SKIP.MSG
                  IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:RTN.STRING)
                  ID.ARR(I) = "" ; * RETURN
               END
            CASE WU # ""
               WU = TRIM(WU)
               IF WU = "" THEN WU=0
               IF WU # 1 AND WU # 0 THEN
                  RTN.STRING = "412-Precondition Failed: WU [":WU:"] must be 1 or 0. ":SKIP.MSG
                  IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:RTN.STRING)
                  ID.ARR(I) = "" ; * RETURN
               END
         END CASE
         IF RTN.STRING # "" THEN RELEASE; RETURN
         nbrELEMENTS += 1
      NEXT I
      
      *                                                                           
      * Pass # 2 : Database Updates - use uv transaction logging                  
      *                                                                           
      ******UV.LOGGING = SYSTEM(60)                                               
      *---------------------------------------------------------------------------
      * if not upl.logging :: keep a "REC.COPY" and re-write it in case of error  
      * MUST reinvent a B4log RRlog construct for business-event level logging    
      *---------------------------------------------------------------------------
      
      IF UPL.LOGGING THEN
$IFDEF isRT
         TRANSTART SETTING tSTATUS THEN
            RTN.STRING = ""
            GOSUB WRITE..THE..DATA
            IF RTN.STRING # "" THEN 
               LOG.MSG = "997-TRANSABORT write failures ":RTN.STRING
               IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
               TRANSABORT ELSE
                  LOG.MSG = "997-TRANSABORT failed. STATUS(":STATUS():") ":RTN.STRING
                  IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
                  RELEASE
                  RETURN
               END
            END
            TRANSEND ELSE
               LOG.MSG = "998-TRANSEND failed. STATUS(":STATUS():")"
               IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
               TRANSABORT ELSE
                  LOG.MSG = "998-TRANSABORT failed. STATUS(":STATUS():")"
                  IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
                  RELEASE
                  RETURN
               END
            END
         END ELSE
            LOG.MSG = "999-TRANSTART failed. STATUS(":STATUS():")"
            IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
            TRANSABORT ELSE
               LOG.MSG = "999-TRANSABORT failed. STATUS(":STATUS():")"
               IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
               RELEASE
               RETURN
            END
         END
$ELSE
         BEGIN TRANSACTION
            RTN.STRING = ""
            GOSUB WRITE..THE..DATA
            IF RTN.STRING # "" THEN ROLLBACK
            COMMIT ELSE
               RTN.STRING = "999-COMMIT failed. STATUS(":STATUS():")"
               ROLLBACK
            END
         END TRANSACTION
$ENDIF
      END ELSE
         GOSUB WRITE..THE..DATA
      END
      *---------------------------------------------------------------------------
      IF RTN.STRING # "" THEN CALL uLOGGER(0, LOG.KEY:RTN.STRING)
      ETX = TIME()
      DIFF= ETX - STX
      LOG.KEY = MEMORY.VARS(1):@FM
      LOG.MSG = "   SR_DBWRITER_UV Finished ":nbrELEMENTS:" write group(s) in ":DIFF:" seconds"
      IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
      RETURN
      *---------------------------------------------------------------------------
WRITE..THE..DATA:
      TRY=0
      LCK.MSG = ""
      FOR I = 1 TO nbrELEMENTS
$IFDEF isRT
         FILE.HNAME = FILEPATH(FILE.ARR(I))<3>
         IF FILE.HNAME = "" AND FILEPATH(FILE.ARR(I))<3> = 1 THEN FILE.HNAME = "MD"
$ELSE
         FILE.HNAME = FILEINFO(FILE.ARR(I), 1)
$ENDIF         
         IF ID.ARR(I) = "" THEN CONTINUE
         IF NOT(UPL.LOGGING) THEN
            LOG.MSG = ">> SR_DBWRITER_UV writing [":ID.ARR(I):"] to ":FILE.HNAME:LCK.MSG
            IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
         END
         wSTATUS=0
         LCK.MSG = ""
         IF WU THEN
            IF WV # "" THEN
$IFDEF isRT
               WRITEVU REC.ARR(I) ON FILE.ARR(I), ID.ARR(I), WV   SETTING wSTATUS ON ERROR wSTATUS=1 
$ELSE
               WRITEVU REC.ARR(I) ON FILE.ARR(I), ID.ARR(I), WV   ON ERROR wSTATUS=1 LOCKED wSTATUS=2 THEN wSTATUS=0 ELSE wSTATUS=3
$ENDIF
            END ELSE
$IFDEF isRT
               WRITEU  REC.ARR(I) ON FILE.ARR(I), ID.ARR(I)       SETTING wSTATUS ON ERROR wSTATUS=1 
$ELSE
               WRITEU  REC.ARR(I) ON FILE.ARR(I), ID.ARR(I)       ON ERROR wSTATUS=1 LOCKED wSTATUS=2 THEN wSTATUS=0 ELSE wSTATUS=3
$ENDIF
            END
         END ELSE
            IF WV # "" THEN
$IFDEF isRT
               WRITEV REC.ARR(I) ON FILE.ARR(I), ID.ARR(I), WV    SETTING wSTATUS ON ERROR wSTATUS=1 
$ELSE
               WRITEV REC.ARR(I) ON FILE.ARR(I), ID.ARR(I), WV    ON ERROR wSTATUS=1 LOCKED wSTATUS=2 THEN wSTATUS=0 ELSE wSTATUS=3
$ENDIF
            END ELSE
$IFDEF isRT
               WRITE  REC.ARR(I) ON FILE.ARR(I), ID.ARR(I)        SETTING wSTATUS ON ERROR wSTATUS=1 
$ELSE
               WRITE  REC.ARR(I) ON FILE.ARR(I), ID.ARR(I)        ON ERROR wSTATUS=1 LOCKED wSTATUS=2 THEN wSTATUS=0 ELSE wSTATUS=3
$ENDIF
            END
         END
         *
         BEGIN CASE
            CASE wSTATUS = 0
               RTN.STRING = ""
               RETURN
            CASE wSTATUS = 1
               RTN.STRING = "990-ON ERROR clause taken. STATUS(":STATUS():")"
               RETURN
            CASE wSTATUS = 2
               ************************************************************
               **** Repeat this iteration for the file and item.        ***
               ****                  :BEWARE:                           ***
               ************************************************************
               TRY += 0.25
               IF TRY > LOCK.WAIT THEN
                  RTN.STRING = "995-Record is locked by terminal number (":STATUS():")"
                  RETURN
               END
               SLEEP 0.25
               I -= 1
               LCK.MSG = "  retry (":TRY:")  waiting on user: ":STATUS()
               IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LCK.MSG)
            CASE wSTATUS = 3
               RTN.STRING = "992-WRITE ELSE clause taken. STATUS(":STATUS():") on file [":FILE.HNAME:"]"
               RETURN
         END CASE
      NEXT I
      RETURN
   END

