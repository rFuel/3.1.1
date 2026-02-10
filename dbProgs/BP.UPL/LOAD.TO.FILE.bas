      * ***************************************************************
$INCLUDE I_Prologue
      * Make sure common is set up before running this program in ******
      * ----------------------------------------------------------------
      * End-User example of a PROGRAM executed from a micro-service     
      * ----------------------------------------------------------------
      * EXAMPLE: micro-service line is :-                               
      * exec-t-LOAD.TO.FILE <tm> dpool=@DPOOL<tm> vpool=@VPOOL<tm> mpool=@MPOOL<tm>
      * ----------------------------------------------------------------
      LOG.KEY = MEMORY.VARS(1):@FM
      PRECISION 9
      STX = TIME()
      LOG.MSG = "   LOAD.TO.FILE Started "
      IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
      CALL SR.FILE.OPEN (ERR, "VOC", VOC)
      IF ERR # "" THEN
         IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:"Cannot open VOC")
         PRINT "Cannot open VOC"
         STOP
      END
      *
      DIM REC.ARR(100) ; MAT REC.ARR = ""
      DIM FILE.ARR(100) ; MAT FILE.ARR = ""
      DIM ID.ARR(100) ; MAT ID.ARR = ""
      DIM WV.ARR(100) ; MAT WV.ARR = ""
      DIM WU.ARR(100) ; MAT WU.ARR = ""
      * ----------------------------------------------------------------
      DIM INS.ARR(100) ; MAT INS.ARR = 1           ; * default is INSERT
      * ----------------------------------------------------------------
      * Strip input parameters from @SENTENCE
      * ----------------------------------------------------------------
      EXECUTE "WHO" CAPTURING JUNK
$IFDEF isRT
      WHOAMI = FIELD(FIELD(JUNK, ",", 2), ")", 1)
$ELSE
      WHOAMI = FIELD(JUNK, " ", 2)
$ENDIF
      pAns = ""
      CALL SR.GET.PROPERTY("allow.append", pAns)
      IF UPCASE(pAns)="FALSE" THEN
         pAns=0
      END ELSE
         IF UPCASE(pAns)="TRUE" THEN
            pAns = 1
         END ELSE
            IF NOT(NUM(pAns)) THEN
               MSG = "   [":pAns:"] is invalid for allow.append - defaulting to OFF"
               CALL uLOGGER(0, LOG.KEY:LOG.MSG)
               pAns=0
            END
         END
      END
      ALLOW.APPEND = pAns + 0
      CALL SR.GET.PROPERTY("dirty.updates", pAns)
      IF UPCASE(pAns)="FALSE" THEN
         pAns=0
      END ELSE
         IF UPCASE(pAns)="TRUE" THEN
            pAns = 1
         END ELSE
            IF NOT(NUM(pAns)) THEN
               MSG = "   [":pAns:"] is invalid for dirty.updateson  - defaulting to OFF"
               IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
               pAns=0
            END
         END
      END
      ALLOW.DIRTY.UPDATES = pAns + 0
      CALL SR.GET.PROPERTY("subs.fm", pAns)
      IF pAns # "" THEN SUBS.FM = pAns ELSE SUBS.FM = "@FM@"
      CALL SR.GET.PROPERTY("subs.vm", pAns)
      IF pAns # "" THEN SUBS.VM = pAns ELSE SUBS.VM = "@VM@"
      CALL SR.GET.PROPERTY("subs.sm", pAns)
      IF pAns # "" THEN SUBS.SM = pAns ELSE SUBS.SM = "@SM@"
      CALL SR.GET.PROPERTY("dacct", pAns)
      DACCT = pAns
      CALL SR.GET.PROPERTY("msgid", MSGID)
      PROG = "LOAD.TO.FILE"
      apiGOOD = "200"
      DIFFERENT.ACCOUNT = 0
      IF DACCT # "" AND UPCASE(DACCT) # UPCASE(WHOAMI) THEN DIFFERENT.ACCOUNT = 1
      IF DIFFERENT.ACCOUNT THEN
         LOG.MSG = "      Data Account: [":DACCT:"]   rFuel running in [":WHOAMI:"]"
         IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
      END
      *
      REMOVE.QFILE = ""
$IFDEF isRT
      INCMD = SENTENCE()
$ELSE
      INCMD = @SENTENCE
$ENDIF
      XCMD = INCMD
      CONVERT " " TO @FM IN XCMD
      LOCATE(PROG, XCMD; POS) ELSE STOP
      POS -= 1
      FOR X = 1 TO POS
         XCMD = DELETE(XCMD, 1, 0, 0)
      NEXT X
      CONVERT @FM TO " " IN XCMD
      INCMD = XCMD
      INCMD = INCMD[INDEX(INCMD, " ",1)+1, LEN(INCMD)]
      CMD = ""
      SEP = "<tm>"
      CALL SR.GET.INSTRINGS (RTN.STRING , INCMD , SEP , CMD)
      CONVERT @SM TO @FM IN  CMD
      CMD = EREPLACE(CMD, "<fm>", @FM)
      CMD = EREPLACE(CMD, "<vm>", @VM)
      CMD = EREPLACE(CMD, "<sm>", @SM)
      * ----------------------------------------------------------------
      VPOOL = "X"
      MPOOL = "X"
      DPOOL = "X"
      RTN.CODE = ""
      EOI = DCOUNT(CMD, @FM)
      FOR I = 1 TO EOI
         LINE = CMD<I>
         CHK = FIELD(LINE, "=", 1)
         BEGIN CASE
            CASE CHK = "vpool"
               VPOOL = LINE[LEN(CHK)+2,LEN(LINE)]
            CASE CHK = "mpool"
               MPOOL = LINE[LEN(CHK)+2,LEN(LINE)]
            CASE CHK = "dpool"
               DPOOL = LINE[LEN(CHK)+2,LEN(LINE)]
            CASE UPCASE(CHK) = "RUNSTATUS"
               RTN.CODE = LINE[LEN(CHK)+2,LEN(LINE)]
         END CASE
      NEXT I
      IF RTN.CODE # "" AND RTN.CODE[1,3] # apiGOOD THEN GO END..PROGRAM
      * ----------------------------------------------------------------
      BASE.ERROR = " Malformed micro-service: "
      ERROR.FND = ""
      IF MPOOL = "X" THEN ERROR.FND := BASE.ERROR:" no DB-Map provided "
      IF DPOOL = "X" THEN ERROR.FND := BASE.ERROR:" no data sent to program "
      IF ERROR.FND # "" THEN
         IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:"   ":ERROR.FND)
         PRINT ERROR.FND
         GO END..PROGRAM
      END
      * ----------------------------------------------------------------
      *  Get the Keys and read for existing records in case of UPDATE   
      * ----------------------------------------------------------------
      CONVERT @VM TO @FM IN VPOOL
      CONVERT @VM TO @FM IN MPOOL
      CONVERT @VM TO @FM IN DPOOL
      FILE.NAMES = "" ; FILES.SEEN = ""
      EOI = DCOUNT(MPOOL, @FM)
      FOR M = 1 TO EOI
         LINE = TRIM(MPOOL<M>)
         IF LINE = '' THEN CONTINUE
         AV = FIELD(LINE, "_", 3)
         FILE = FIELD(LINE, "_", 1)
         ufPOS = INDEX(LOCAL.FILES, " ":FILE:" ", 1)
         IF DIFFERENT.ACCOUNT AND NOT(ufPOS) THEN
*********   VKEY = "upl_":FILE:"_":DACCT
            VKEY = "upl_":FILE:"_":DACCT:"_":MSGID
            FILE = VKEY
         END
         FILE.ERR = 0
$IFDEF isRT
         IF FILE = "VOC" THEN FILE = "MD"
         LOCATE FILE IN FILE.NAMES SETTING FPOS ELSE 
            FILE.NAMES<-1> = FILE
            LOCATE FILE IN FILE.NAMES SETTING FPOS ELSE FILE.ERR = 1
         END
$ELSE
         LOCATE(FILE, FILE.NAMES; FPOS) ELSE 
            FILE.NAMES<-1> = FILE
            LOCATE(FILE, FILE.NAMES; FPOS) ELSE FILE.ERR = 1
         END
$ENDIF
         IF FILE.ERR THEN
            ERR.MSG = " LOAD.TO.FILE: LOCATE Error: on file [":FILE:"]."
            IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:">>> ":ERR.MSG)
            PRINT ERR.MSG
            GO END..PROGRAM
         END
         IF AV = 0 THEN
            ERR = ""
            CALL SR.FILE.OPEN(ERR, FILE, FILEvar)
            IF ERR # "" THEN
               ERR.MSG = " LOAD.TO.FILE: Error: file [":FILE:"] does not exist."
               IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:">>> ":ERR.MSG)
               PRINT ERR.MSG
               GO END..PROGRAM
            END
            FILE.ARR(FPOS) = FILEvar
            IDvar = DPOOL<M>
            * ----------------------------------------------------------*
            IF IDvar = "" THEN
               ID.ARR(FPOS) = ""
               REC.ARR(FPOS)= ""
               INS.ARR(FPOS) = 1
               CONTINUE
            END
            RECvar = ""
            SETLOCK = 1
            EXISTS = 0
            LOCK.CNT=0
            LOOP
               LOCK.CNT+=1
               IF LOCK.CNT > 15 THEN EXIT
               CALL SR.ITEM.EXISTS (EXISTS, FILEvar, IDvar, RECvar, SETLOCK)
               BEGIN CASE
                  CASE EXISTS=1
                     ID.ARR(FPOS) = IDvar
                     REC.ARR(FPOS)= RECvar
                     INS.ARR(FPOS) = 0
                     EXIT
                  CASE EXISTS=2
                     IF LOCK.CNT<=3 THEN
$IFDEF isRT
                        LOCK.ERROR = "**** Record [":IDvar:"] in file ":FILE:" is currently locked."
$ELSE
                        LOCK.ERROR = "**** Record [":IDvar:"] in file ":FILEINFO(FILEvar, 1):" is currently locked."
$ENDIF
                        IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOCK.ERROR)
                     END
                     IF LOCK.CNT <= 10 THEN RQM ; CONTINUE
                     PRINT LOCK.ERROR
                     GO END..PROGRAM
                  CASE 1
                     IF SETLOCK THEN
                        RELEASE FILE.ARR(FPOS), IDvar
                        SETLOCK = 0
                     END
                     IF ALLOW.DIRTY.UPDATES THEN
                        DIRTY.ERROR = "Dirty Update => this task provided an @ID for a new record."
                        IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:"*** ":DIRTY.ERROR)
                        DIRTY.ERROR = "Dirty Updates is ON. Insert @ID ":IDvar:" into file ":FILE
                        IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:"*** ":DIRTY.ERROR)
                        ID.ARR(FPOS) = IDvar
                        REC.ARR(FPOS)= ""
                        INS.ARR(FPOS) = 0
                     END ELSE
                        DIRTY.ERROR = "Dirty Updates is OFF. Update of non-existant record is not permitted."
                        IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:"*** ":DIRTY.ERROR)
                        DIRTY.ERROR = "Dirty Updates is OFF. Cannot insert @ID ":IDvar:" into file ":FILE
                        IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:"*** ":DIRTY.ERROR)
                        PRINT DIRTY.ERROR
                        GO END..PROGRAM
                     END
                     EXIT
               END CASE
            REPEAT
         END
      NEXT M
      * ----------------------------------------------------------------
      * Update records: inserts MUST start with an empty record        *
      * ----------------------------------------------------------------
      FOR M = 1 TO EOI
         LINE = TRIM(MPOOL<M>)
         IF LINE = '' THEN CONTINUE
         * -------------------------------------------------------------
         * Get the file into which data will be written                *
         * LINE should look like filename_F_a_m_s_{iconv}              *
         * -------------------------------------------------------------
         FILE = FIELD(LINE, "_", 1)
         ufPOS = INDEX(LOCAL.FILES, " ":FILE:" ", 1)
         IF DIFFERENT.ACCOUNT AND NOT(ufPOS) THEN
*********   VKEY = "upl_":FILE:"_":DACCT
            VKEY = "upl_":FILE:"_":DACCT:"_":MSGID
            FILE = VKEY
         END
$IFDEF isRT
         IF FILE = "VOC" THEN FILE = "MD"
$ENDIF
         FND = 1
$IFDEF isRT
         LOOKIN  = FILE.NAMES
         LOCATE FILE IN LOOKIN SETTING FPOS ELSE FND = 0
$ELSE
         LOOKIN  = FILE.NAMES
         LOCATE(FILE, LOOKIN; FPOS) ELSE FND = 0
$ENDIF
         IF NOT(FND) THEN
            ERR.MSG = " LOAD.TO.FILE: Logic Error on file [":FILE:"] - ensure all IDs are mapped."
            IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:">>> ":ERR.MSG)
            PRINT ERR.MSG
            GO END..PROGRAM
         END
         AV = UPCASE(FIELD(LINE, "_", 3))
         MV = UPCASE(FIELD(LINE, "_", 4))
         SV = UPCASE(FIELD(LINE, "_", 5))
         CV = FIELD(LINE, "_", 6)
         * ------------------------------------------------------------- *
         *  The following lines are NOT sympathetic to the x-n notation  *
         *                    *** TO BE DEVELOPED ***                    *
         * ------------------------------------------------------------- *
         IF (NOT(NUM(AV)) AND AV#"N") OR AV="" THEN CONTINUE
         IF (NOT(NUM(MV)) AND MV#"N") OR MV="" THEN MV = 1
         IF (NOT(NUM(SV)) AND SV#"N") OR SV="" THEN SV = 1
         * -------------------------------------------------------------
         * Update or Create the Record
         * -------------------------------------------------------------
         IF AV="N" THEN oAV=1 ELSE oAV=AV
         IF MV="N" THEN oMV=1 ELSE oMV=MV
         IF SV="N" THEN oSV=1 ELSE oSV=SV
         IF (AV # 0) THEN
            OLD.REC = REC.ARR(FPOS)<oAV, oMV, oSV>
            VAR = DPOOL<M>
            IF VAR="" THEN VAR = OLD.REC
            * use " " in the payload to clear a field
            IF VAR=" " THEN VAR=""
            * ---------------------------------------------------------
            VAR = EREPLACE(VAR, "<sm>", SUBS.SM)
            VAR = EREPLACE(VAR, "<vm>", SUBS.VM)
            VAR = EREPLACE(VAR, "<fm>", SUBS.FM)
            * ---------------------------------------------------------
            * Handle multiple occurences of a value - e.g. append (-1)
            * THIS IS DANGEROUS - do not publish this capability !!!
            * ---------------------------------------------------------
            TVAR = EREPLACE( VAR, SUBS.FM, @FM)
            TVAR = EREPLACE(TVAR, SUBS.VM, @VM)
            TVAR = EREPLACE(TVAR, SUBS.SM, @SM)
            IF ALLOW.APPEND THEN
               IF TVAR # VAR THEN SUBS.FLAG=1 ELSE SUBS.FLAG=0
               BLOK = ""
               CONVERT TILDE TO "," IN CV
               EOA = DCOUNT(TVAR, @FM)
               AMARK = "" ; VMARK = "" ; SMARK = ""
               FOR Aval = 1 TO EOA
                  IF Aval > 1 THEN AMARK = @FM
                  EOM = DCOUNT(TVAR<Aval>, @VM)
                  FOR Mval = 1 TO EOM
                     IF Mval > 1 THEN VMARK = @VM
                     EOS = DCOUNT(TVAR<Aval, Mval>, @SM)
                     * ----------------------------------------------------
                     * I want this line in the code BUT is affects the 050 
                     * read operation and fails Autotest. Keep it for rel 3
                     * The affect is correct - it allows inserting of empty
                     * fields (EOS=0) which nobody has requested or tested.
***                  IF EOS = 0 THEN  EOS = 1
                     * ----------------------------------------------------
                     FOR Sval = 1 TO EOS
                        IF Sval > 1 THEN SMARK = @SM
                        PRV.REC = OLD.REC<Aval,Mval,Sval>
                        DATUM = TVAR<Aval, Mval, Sval>
                        IF CV # "" THEN
                           NEW.REC = PRV.REC
                           ERR = ""
                           CALL SR.ICONV(ERR, CV, NEW.REC, PRV.REC, DATUM)
                           IF ERR # "" THEN
                              IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:ERR)
                              PRINT ERR
                              GO END..PROGRAM
                           END
                           DATUM = NEW.REC
                        END
                        BLOK := AMARK:VMARK:SMARK:DATUM
                        AMARK = "" ; VMARK = "" ; SMARK = ""
                     NEXT Sval
                     SMARK = ""
                  NEXT Mval
                  VMARK = ""
               NEXT Aval
               IF SUBS.FLAG THEN OLD.VAR = REC.ARR(FPOS)<oAV, oMV, oSV> ELSE OLD.VAR = ""
               REC.ARR(FPOS)<oAV,oMV,oSV> = OLD.VAR:BLOK
            END ELSE
               DATUM = TVAR
               IF CV # "" THEN 
                  NEW.REC = OLD.REC ; * will be reset in SR.ICONV
                  ERR = ""
                  CALL SR.ICONV(ERR, CV, NEW.REC, OLD.REC, DATUM)
                  IF ERR # "" THEN
                     IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:ERR)
                     PRINT ERR
                     GO END..PROGRAM
                  END
                  DATUM = NEW.REC
               END
               REC.ARR(FPOS)<oAV,oMV,oSV> = DATUM
            END
         END
      NEXT M
      * ----------------------------------------------------------------
      *                       Write to Files                           *
      * ----------------------------------------------------------------
      DIM REC.TMP(100)
      DIM FILE.TMP(100)
      DIM ID.TMP(100)
      DIM WU.TMP(100)
      DIM WV.TMP(100)
      FOR M = 1 TO 100
         IF REC.ARR(M) = "" THEN CONTINUE
         * -------------------------------------------------------------
         MAT REC.TMP = ""
         MAT FILE.TMP = ""
         MAT ID.TMP = ""
         MAT WU.TMP = ""
         MAT WV.TMP = ""
         * -------------------------------------------------------------
         INSERT.SW = 0
         IF ID.ARR(M) = "" THEN INSERT.SW = 1
         * -------------------------------------------------------------
         REC.TMP(1) = REC.ARR(M)
         FILE.TMP(1) = FILE.ARR(M)
         ID.TMP(1) = ID.ARR(M)
         WU.TMP(1) = WU.ARR(M)
         WV.TMP(1) = WV.ARR(M)
         * -------------------------------------------------------------
         IF (INSERT.SW) THEN
            * use uCATALOG GetNextID.{filename} or SEQUENTIAL.ID
            CALL SR.INSERT.RECORD (RTN.STRING, MAT REC.TMP, MAT FILE.TMP, MAT ID.TMP, MAT WV.TMP, MAT WU.TMP)
         END ELSE
            CALL SR_DBWRITER_UV (RTN.STRING, MAT REC.TMP, MAT FILE.TMP, MAT ID.TMP, MAT WV.TMP, MAT WU.TMP)
         END
         IF RTN.STRING # "" THEN
            PRINT RTN.STRING
            GO END..PROGRAM
         END
      NEXT M
      RELEASE
      ETX = TIME()
      DIFF= ETX - STX
      IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:"   LOAD.TO.FILE Finished in ":DIFF:" seconds")
END..PROGRAM:
      REMOVE.QFILE<-1> = FILE.NAMES
      EOI = DCOUNT(REMOVE.QFILE, @FM)
      FOR I = 1 TO EOI
         IF REMOVE.QFILE<I> = "" THEN CONTINUE
******   DELETE VOC, REMOVE.QFILE<I>
         CALL SR.FILE.CLOSE(ERR, REMOVE.QFILE<I>)
         IF ERR # "" THEN
            MSG = " LOAD.TO.FILE: SR.FILE.CLOSE Error [":ERR:"]"
            IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:MSG)
            EXIT
         END
      NEXT I
$IFNDEF isRT
      CLOSE
$ENDIF
      STOP
      * ----------------------------------------------------------------
GET..FILE..HANDLE:
      ERR = ""
      CALL SR.FILE.OPEN(ERR, FILE, HANDLE)
      
      IF ERR # "" THEN
         MSG = " LOAD.TO.FILE: File Open Error [":ERR:"]"
         IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:MSG)
         MSG = ERR:" [":FILE:"]"
         PRINT MSG
         READ VREC FROM VOC, FILE THEN
            IF VREC<1> = "Q" AND FILE[1,4] = "upl_" THEN DELETE VOC, FILE
         END
         GO END..PROGRAM
      END
      
      RETURN
   END
