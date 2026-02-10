      SUBROUTINE SR.INSERT.RECORD (RTN.STRING, MAT REC.IN, MAT FILE.IN, MAT ID.IN, MAT WV.IN, MAT WU.IN)
$INCLUDE I_Prologue
      * ----------------------------------------------------------------
$IFDEF isRT
      DIM REC.IN(100)
      DIM FILE.IN(100)
      DIM ID.IN(100)
      DIM WU.IN(100)
      DIM WV.IN(100)
$ENDIF
      * ----------------------------------------------------------------
      PRECISION 9
      STX = TIME()
      LOG.KEY = MEMORY.VARS(1):@FM
      LOG.MSG = "   >> SR.INSERT.RECORD Started ***************"
      IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
      
      MS.PREFIX = "GetNextID."
      OK  = "200"
      ERR = 0
      LOG.KEY = MEMORY.VARS(1):@FM
      LOG.MSG = "   SR.INSERT.RECORD Started "
      IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
      CALL SR.FILE.OPEN(ERR, "uCATALOG", uCATALOG)
      IF ERR THEN
         RTN.STRING = "412-Precondition Failed: unOPENed file handle on [uCATALOG]"
         IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:REPLY)
         RETURN
      END
      DIM REC.ARR(100)
      DIM FILE.ARR(100)
      DIM ID.ARR(100)
      DIM WU.ARR(100)
      DIM WV.ARR(100)
      RTN.STRING = ""
      * ----------------------------------------------------------------
      FOR I = 1 TO 100
         IF REC.IN(I) = "" THEN EXIT
         MAT REC.ARR  = "" 
         MAT FILE.ARR = ""
         MAT ID.ARR   = ""
         MAT WU.ARR   = ""
         MAT WV.ARR   = ""
         * -------------------------------------------------------------
         * Get an @ID for each write.                                   
         *   Use a micro-service to get the id. The msv is file based   
         *   e.g. GetNextID.CLIENT   or GetNextID.TRAN   etc.           
         * -------------------------------------------------------------
$IFDEF isRT
         IF FILEINFO(FILE.IN(I))  = "" THEN NOT.OPEN=1 ELSE NOT.OPEN=0
$ELSE
         IF FILEINFO(FILE.IN(I),0) = 0 THEN NOT.OPEN=1 ELSE NOT.OPEN=0
$ENDIF
         IF NOT.OPEN THEN 
$IFDEF isRT
            FLCAT = MS.PREFIX:FILEPATH(FILE.IN(I))<4>
$ELSE
            FLCAT = MS.PREFIX:FILEINFO(FILE.IN(I),1)
$ENDIF
            THIS.ID = "ERROR-CONDITION"
            ORDER   = "" ; SRTNS = "" ; PRGMS = ""
            * ----------------------------------------------------------
            CALL SR.PREPARE.MSERVICE(RTN.CODE, RTN.MSG, FLCAT, ORDER, SRTNS, PRGMS)
            IF RTN.CODE # "" THEN
               GOSUB SEQUENTIAL..ID    ; * No msv created for the file  
            END ELSE
               * -------------------------------------------------------
               CALL SR.EXECUTE.MSERVICE (REPLY, FLCAT, ORDER, SRTNS, PRGMS, VPOOL, MPOOL, DPOOL)
               IF REPLY # "" AND REPLY # 0 THEN
                  IF REPLY[1,4] = OK:"-" THEN
                     POSN = INDEX(REPLY, "-", 1) + 1
                     THIS.ID = REPLY[POSN, LEN(REPLY)]
                  END ELSE
                     RTN.STRING = REPLY
                     IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:REPLY)
                     RETURN
                  END
               END
            END
            IF THIS.ID # "ERROR-CONDITION" THEN
               ID.ARR(1)  = THIS.ID
               REC.ARR(1) = REC.IN(I)
               FILE.ARR(1)= FILE.IN(I)
               WU.ARR(1)  = WU.IN(I)
               WV.ARR(1)  = WV.IN(I)
               CALL SR_DBWRITER_UV (RTN.STRING, MAT REC.ARR, MAT FILE.ARR, MAT ID.ARR, MAT WV.ARR, MAT WU.ARR)
            END
         END
      NEXT I
      ETX = TIME()
      DIFF= ETX - STX
      LOG.MSG = "   >> SR.INSERT.RECORD Finished ************** in ":DIFF:" seconds" 
      IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
      
      RETURN
      * ----------------------------------------------------------------
SEQUENTIAL..ID:
      THIS.ID = 1
      LOOP
         READ CHK FROM FILE.IN(I), THIS.ID ELSE EXIT
         THIS.ID += 1
      REPEAT
      RETURN
   END
