      SUBROUTINE SR.WRITETOLAYOUT (MAT IN.STRINGS)
$INCLUDE I_Prologue
      * --------------------------------------------------------
      * Equate local variables to call string locations         
      * --------------------------------------------------------
$IFDEF isRT
      DIM IN.STRINGS(20)
$ENDIF
      PRECISION 9
      STX = TIME()
      EQU REPLY TO IN.STRINGS(1)
      EQU FILE  TO IN.STRINGS(2)
      EQU VPOOL TO IN.STRINGS(3)
      EQU MPOOL TO IN.STRINGS(4)
      EQU DPOOL TO IN.STRINGS(5)
      EQU EIOBJ TO IN.STRINGS(6)                 ; * true / false
      DIM REC.ARR(100)
      DIM FILE.ARR(100)
      DIM ID.ARR(100)
      DIM WV.ARR(100)
      DIM WU.ARR(100)
      LOG.KEY = MEMORY.VARS(1):@FM
      LOG.MSG = "   SR.WRITETOLAYOUT Started for file  [":FILE:"]"
      IF INF.LOGGING THEN CALL uLOGGER(3, LOG.KEY:LOG.MSG)
      * --------------------------------------------------------
      REPLY = ""
      ERR = 0
      CALL SR.OPEN.CREATE (ERR, "UPL.LAYOUTS", "", LAYOUTS)
      IF ERR THEN
         REPLY = "   SR.WRITETOLAYOUT FAIL: UPL.LAYOUTS is missing"
         IF INF.LOGGING THEN CALL uLOGGER(3, LOG.KEY:REPLY)
         RETURN
      END
      *
      IF (EIOBJ) THEN
         * --------------------------------------------------------
         * Open the data structures                                
         * --------------------------------------------------------
         IMPORT.FN = "IMPORT.":FILE
         INDEX.FN = "INDEX.":FILE
         HISTORY.FN= "HISTORY.":FILE
         FILE.ERR = 0
         cSTX = TIME()
         IF NOT(FILE.ERR) THEN CALL SR.OPEN.CREATE(FILE.ERR, IMPORT.FN, "", IMPORT.FILE)
         cETX = TIME()
         LOG.MSG = "-- SR.WRITETOLAYOUT call SR.OPEN.CREATE ":cETX - cSTX:" seconds"
         IF INF.LOGGING THEN CALL uLOGGER(3, LOG.KEY:LOG.MSG)
         cSTX = TIME()
         IF NOT(FILE.ERR) THEN CALL SR.OPEN.CREATE(FILE.ERR, INDEX.FN, "", INDEX.FILE)
         cETX = TIME()
         LOG.MSG = "-- SR.WRITETOLAYOUT call SR.OPEN.CREATE ":cETX - cSTX:" seconds"
         IF INF.LOGGING THEN CALL uLOGGER(3, LOG.KEY:LOG.MSG)
         cSTX = TIME()
         IF NOT(FILE.ERR) THEN CALL SR.OPEN.CREATE(FILE.ERR, HISTORY.FN, "", HISTORY.FILE)
         cETX = TIME()
         LOG.MSG = "-- SR.WRITETOLAYOUT call SR.OPEN.CREATE ":cETX - cSTX:" seconds"
         IF INF.LOGGING THEN CALL uLOGGER(3, LOG.KEY:LOG.MSG)
         IF (FILE.ERR) THEN
            REPLY = "SR.WRITETOLAYOUT cannot access EIO structures for [":FILE:"]"
            IF INF.LOGGING THEN CALL uLOGGER(3, LOG.KEY:REPLY)
            RETURN
         END
         rdSTX = TIME()
         READ LAYREC FROM LAYOUTS, FILE ELSE
            REPLY = "   SR.WRITETOLAYOUT loading ":FILE:" blind - no layout found"
            IF INF.LOGGING THEN CALL uLOGGER(3, LOG.KEY:REPLY)
            LAYREC = "" ; REPLY = ""
***         RETURN
         END
         rdETX = TIME()
      END ELSE
         * --------------------------------------------------------
         * Open the recipient uISO file - Information Store Object 
         *     uISO files are customer files in the user database  
         * --------------------------------------------------------
         CALL SR.OPEN.CREATE (ERR, FILE, "", IOFILE)
         IF ERR THEN
            REPLY = "SR.WRITETOLAYOUT FAIL: File ":FILE:" cannot open file"
            IF INF.LOGGING THEN CALL uLOGGER(3, LOG.KEY:REPLY)
            RETURN
         END
         rdSTX = TIME()
         READ LAYREC FROM LAYOUTS, FILE ELSE
            REPLY = "SR.WRITETOLAYOUT FAIL: File ":FILE:" was not found in UPL.LAYOUTS"
            IF INF.LOGGING THEN CALL uLOGGER(3, LOG.KEY:REPLY)
            RETURN
         END
         rdETX = TIME()
      END
      LOG.MSG = "-- SR.WRITETOLAYOUT READ LAYREC---------------":rdETX - rdSTX:" seconds"
      IF INF.LOGGING THEN CALL uLOGGER(3, LOG.KEY:LOG.MSG)
      rETX = TIME()
      LOG.MSG = "-- SR.WRITETOLAYOUT run-time******************":rETX - STX:" seconds"
      IF INF.LOGGING THEN CALL uLOGGER(3, LOG.KEY:LOG.MSG)
      
      * ------------------------------------------------------------------
      * Create the data record                                            
      * ------------------------------------------------------------------
      VLIST = CONVERT(CHAR(9), @FM, VPOOL)
      MLIST = CONVERT(CHAR(9), @FM, MPOOL)
      DLIST = CONVERT(CHAR(9), @FM, DPOOL)
      EOI = DCOUNT(VLIST, @FM)
      
      BLIND.LOAD = (LAYREC='')
      IF BLIND.LOAD THEN LAYREC = @VM:1:@VM:1:@VM:1:@VM
      
*      PRECISION 9
      rSTX = TIME()
      KEY = ""
      OUTREC = ""
      FOR I = 1 TO EOI
         REMOVE VAR FROM DLIST SETTING STOP.SW
         REMOVE VVR FROM VLIST SETTING STOP.SW
         IF BLIND.LOAD THEN
            ANS=1
            LAYREC<1,2> = I
         END ELSE
            CALL SR.FIND.IN.LAYOUT(ANS, LAYREC, VVR)
            IF ANS = 0 THEN CONTINUE
         END
         *
         AV = LAYREC<ANS,2>
         MV = LAYREC<ANS,3>
         SV = LAYREC<ANS,4>
         CV = LAYREC<ANS,6>
         * ---------------------------------------------------------------
         * TEST condition only. Payloads can and will pass in the @ID     
         * E.G. @ID   in payload called "AccountNumber"                   
         * ---------------------------------------------------------------
         IF MV = "" THEN MV = 1
         IF SV = "" THEN SV = 1
         *
         IF MV = 0 THEN MV=1
         IF SV = 0 THEN SV=1
         *
         IF UPCASE(MV) = "N" THEN MV = -1
         IF UPCASE(SV) = "N" THEN SV = -1
         *
         * VAR = DLIST<I>
         IF CV # "" THEN VAR = ICONV(VAR, CV)
         *
         IF BLIND.LOAD THEN VAR = VVR:"=":VAR
         *
         IF AV = 0 THEN 
            KEY = VAR
         END ELSE 
            OUTREC<AV,MV,SV> = VAR
         END
      NEXT I
      
      * -------------------------------------------------------------------
      * Create the key as per layout specifications                        
      * Keys are obtained from either;                                     
      *     a) the data pool                                               
      *     b) a srtn call                                                 
      *     c) from values inside OUTREC                                   
      * -------------------------------------------------------------------
      
      rETX = TIME()
      rDIFF= rETX = rSTX
      LOG.MSG = "      >> SR.WRITETOLAYOUT Loaded records         in ":rDIFF:" seconds"
      IF INF.LOGGING THEN CALL uLOGGER(3, LOG.KEY:LOG.MSG)
      LOG.MSG = "-- SR.WRITETOLAYOUT run-time******************":rETX - STX:" seconds"
      IF INF.LOGGING THEN CALL uLOGGER(3, LOG.KEY:LOG.MSG)
      SEP = ""
      IF KEY = "" AND NOT(BLIND.LOAD) THEN 
         CALL SR.FIND.IN.LAYOUT(ANS, LAYREC, "@ID")
         KEY.DEFN = LAYREC<ANS,5>
         KEY.DEFN = CONVERT("}", @FM, KEY.DEFN)        ; * A-M-S}A-M-S}...
         KEY.DEFN = CONVERT(TILDE, @VM, KEY.DEFN)        ; * A-M-S
         *
         IF (KEY.DEFN[1,4] = "srtn") THEN
            REPLY = "SR.WRITETOLAYOUT FAIL: GetKey() from Subroutine call is not implemented yet"
            IF INF.LOGGING THEN CALL uLOGGER(3, LOG.KEY:REPLY)
            RETURN
         END ELSE
            IF (KEY.DEFN<1>[1,4]="sep=") THEN
               SEP = FIELD(KEY.DEFN<1>, "=", 2)
               KEY.DEFN = DELETE(KEY.DEFN, 1,0,0)
            END
            IF SEP = "" THEN SEP = "*"
            *
            EOI = DCOUNT(KEY.DEFN, @FM)
            FOR I = 1 TO EOI
               AV = KEY.DEFN<I,1>+0
               MV = KEY.DEFN<I,2>+0
               SV = KEY.DEFN<I,3>+0
               KEY := OUTREC<AV,MV,SV>
               IF I < EOI THEN KEY := SEP
            NEXT I
         END
      END 
      CHK = TRIM(CONVERT(SEP, "", KEY))
      IF (LEN(CHK) = 0) THEN
         SEP = "*"
         BASE = DATE():SEP:TIME()
         SEQN = 0
         EXISTS = 0
         LOOP
            KEY = BASE:SEP:SEQN
            CALL SR.ITEM.EXISTS (EXISTS, IMPORT.FILE, KEY, JUNKvar, 0)
            IF NOT(EXISTS) THEN EXIT
            SEQN += 1
         REPEAT
      END
      rETX = TIME()
      rDIFF= rETX = rSTX
      LOG.MSG = "      >> SR.WRITETOLAYOUT Loaded records and IDs in ":rDIFF:" seconds"
      IF INF.LOGGING THEN CALL uLOGGER(3, LOG.KEY:LOG.MSG)
      LOG.MSG = "-- SR.WRITETOLAYOUT run-time******************":rETX - STX:" seconds"
      IF INF.LOGGING THEN CALL uLOGGER(3, LOG.KEY:LOG.MSG)
      
      * -------------------------------------------------------------------
      * Write to the data file(s)                                          
      * -------------------------------------------------------------------
      rSTX = TIME()
      MAT REC.ARR = ""
      MAT FILE.ARR = ""
      MAT ID.ARR = ""
      MAT WV.ARR = ""
      MAT WU.ARR = ""
      *
      IF (EIOBJ) THEN
         *
         READ IDX.REC FROM INDEX.FILE, KEY ELSE IDX.REC = ""
         *
         HIST.REC = OUTREC
         *
         REC.ARR(1) = OUTREC
         FILE.ARR(1) = IMPORT.FILE
         ID.ARR(1) = KEY
         *
         REC.ARR(2) = IDX.REC
         FILE.ARR(2) = INDEX.FILE
         ID.ARR(2) = KEY
         *
         REC.ARR(3) = HIST.REC
         FILE.ARR(3) = HISTORY.FILE
         ID.ARR(3) = DATE():"|":TIME():"|":KEY
      END ELSE
         REC.ARR(1) = OUTREC
         FILE.ARR(1) = IOFILE
         ID.ARR(1) = KEY
      END
      *
      *------------------------------------------------------------------------------
      *
      IF UPL.LOGGING THEN
$IFDEF isRT
         CALL SR_DBWRITER_UV (RTN.STRING, MAT REC.ARR, MAT FILE.ARR, MAT ID.ARR, MAT WV.ARR, MAT WU.ARR)
         IF RTN.STRING # "" THEN
            REPLY = RTN.STRING
         END
$ELSE
         BEGIN TRANSACTION
            CALL SR_DBWRITER_UV (RTN.STRING, MAT REC.ARR, MAT FILE.ARR, MAT ID.ARR, MAT WV.ARR, MAT WU.ARR)
            IF RTN.STRING # "" THEN
               REPLY = RTN.STRING
               ROLLBACK
            END
            COMMIT ELSE
               REPLY = "SR.WRITETOLAYOUT ROLLBACK: COMIT Failure: on TRANSACTION block"
               ROLLBACK
            END
         END TRANSACTION
$ENDIF
      END ELSE
         CALL SR_DBWRITER_UV (RTN.STRING, MAT REC.ARR, MAT FILE.ARR, MAT ID.ARR, MAT WV.ARR, MAT WU.ARR)
         IF RTN.STRING # "" AND RTN.STRING # 0 THEN
            REPLY = RTN.STRING
         END
      END
      *------------------------------------------------------------------------------
      *
      rETX = TIME()
      rDIFF= rETX = rSTX
      LOG.MSG = "      >> SR.WRITETOLAYOUT write to database file in ":rDIFF:" seconds"
      IF INF.LOGGING THEN CALL uLOGGER(3, LOG.KEY:LOG.MSG)
      LOG.MSG = "-- SR.WRITETOLAYOUT run-time******************":rETX - STX:" seconds"
      IF INF.LOGGING THEN CALL uLOGGER(3, LOG.KEY:LOG.MSG)
      RELEASE
$IFNDEF isRT
      CLOSE
$ENDIF
      ETX = TIME()
      DIFF= ETX - STX
      LOG.MSG = "   SR.WRITETOLAYOUT Finished for file  [":FILE:"] in ":DIFF:" seconds"
      IF INF.LOGGING THEN CALL uLOGGER (3, LOG.KEY:LOG.MSG)
      RETURN
   END
