      SUBROUTINE SR.READTRANS (ERR, DSD, LOWDATE, ACCOUNTID, CORREL, PAYLOAD)
$INCLUDE I_Prologue
      *
      PROG     = "SR.READTRANS"
      LOG.KEY  = "CDR-OB":@FM
      ERR      = "Unknown Error"
      PAYLOAD  = ""
      MARKER   = "<tm>"
      T.MARK   = ""
      F.MARK   = "<fm>"
      SLISTS   = "&SAVEDLISTS&"
      DBG      = 0
      LII      = ""
      CLI      = ""
      atID     = ""
      *
      IF INDEX(ACCOUNTID, ":", 1) THEN
         KEY = EREPLACE(ACCOUNTID, ":", @FM)
         LII = KEY<1>
         CLI = KEY<2>
         atID= KEY<3>
      END
      ERR = "Bad or Missing Parameters"
      IF DSD         = ""           THEN GO END..SRTN
      IF ACCOUNTID   = ""           THEN GO END..SRTN
      IF CORREL      = ""           THEN GO END..SRTN
      ERR = "Bad structure of customer, use loginID:clientID:accountID."
      IF LII         = ""           THEN GO END..SRTN
      IF CLI         = ""           THEN GO END..SRTN
      IF atID        = ""           THEN GO END..SRTN
      *
      IF INF.LOGGING THEN
         LOG.MSG = "Start SR.READTRANS for ":ACCOUNTID:" ------------------------------"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      *
      chkID = LII:":":CLI
      ERR = ""
      PROC.LIST = ""
      CALL SR.CDR.VERIFY.ACCTS ( ERR, chkID, PROC.LIST )
      IF ERR # "" THEN GO END..SRTN
      LOCATE(atID, PROC.LIST, 1; FND) ELSE
         ERR = "Failed account verification."
         GO END..SRTN
      END
      *
      ERR = "DB file access error"
      CALL SR.FILE.OPEN (ERR, "TRAN"          , TRAN        ) ; IF ERR # "" THEN ERR = " 1 ":ERR ; GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "TRAN.EXT"      , TRANEXT     ) ; IF ERR # "" THEN ERR = " 2 ":ERR ; GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "CDC.ACCOUNT"   , CDC.ACCOUNT ) ; IF ERR # "" THEN ERR = " 3 ":ERR ; GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "PSEUDO.TRAN"   , PSEUDO.TRAN ) ; IF ERR # "" THEN ERR = " 4 ":ERR ; GO END..SRTN
      CALL SR.FILE.OPEN (ERR, SLISTS          , SL          ) ; IF ERR # "" THEN ERR = " 5 ":ERR ; GO END..SRTN
** [11/02/2021] ADD OVERNIGHT.TRAN ***
      CALL SR.FILE.OPEN (ERR, "OVERNIGHT.TRAN", ONIGHT.TRAN ) ; IF ERR # "" THEN ERR = " 6 ":ERR ; GO END..SRTN
** --------------------------------- *
      *
      * --------------------------------------------------
      *     SANITY check before checking Correlation ID  *
      * --------------------------------------------------
      ERR      = ""
      TRX.ID   = atID
      READ RECORD FROM TRAN, atID ELSE 
         ERR = "No such item on TRAN"
         GO END..SRTN
      END
      * --------------------------------------------------
      *  If it has been built, DO NOT build it again    !!
      *  Other process will delete them after 10 minutes !
      * --------------------------------------------------
      READ CHECK FROM SL, CORREL THEN 
         RECCNT  = DCOUNT(CHECK, @FM)
         IF RECCNT > 0 THEN
            IF INF.LOGGING THEN
               LOG.MSG = "   .) Pre-built. Using existing data at [":CORREL:"]"
               CALL uLOGGER(1, LOG.KEY:LOG.MSG)
            END
            PAYLOAD = RECCNT
            GO END..SRTN
         END
      END
      * --------------------------------------------------
      GOSUB I.WANT
      * --------------- [ TRAN ] -------------------------
      IF INF.LOGGING THEN
         LOG.MSG = "   .) looking for records in TRAN"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      PENDING = 0
      FOR I = 1 TO 16
         RECORD = DELETE(RECORD, 1, 0, 0)
      NEXT I
      GOSUB BUILD..PAYLOAD
      IF INF.LOGGING THEN
         LOG.MSG = "   .) ":DCOUNT(PAYLOAD, @FM):" transactions from TRAN loaded."
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      * --------------- [ TRAN.EXT ] ---------------------
      IF INF.LOGGING THEN
         LOG.MSG = "   .) looking for records in TRAN.EXT"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      ACID = atID
      atID = ACID:"/0"
      READ INDEX.REC FROM TRANEXT, atID ELSE
         ERR = "Item [":atID:"] has gone missing from TRAN.EXT."
         GO END..SRTN
      END
      EOE = DCOUNT(INDEX.REC, @FM)
      FOR EXT.ID = EOE TO 1 STEP -1
         IDX.DTE = INDEX.REC<EXT.ID, 1>
         IF IDX.DTE # "" AND IDX.DTE < LOWDATE THEN CONTINUE
         atID = ACID:"/":EXT.ID
         READ RECORD FROM TRANEXT, atID ELSE CONTINUE
         IF DBG THEN
            IF INF.LOGGING THEN
               LOG.MSG = "   .) loading ":atID
               CALL uLOGGER(1, LOG.KEY:LOG.MSG)
            END
         END
         GOSUB BUILD..PAYLOAD
      NEXT EXT.ID
      IF INF.LOGGING THEN
         LOG.MSG = "   .) ":EOE:" records from TRAN.EXT loaded."
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      WRITE "" ON SL, CORREL
      * --------------- [ OVERNIGHT.TRAN ] ---------------
      IF INF.LOGGING THEN
         LOG.MSG = "   .) looking for records in OVERNIGHT.TRAN"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      SELECT ONIGHT.TRAN
      OTRX = 0
      LOOP
         READNEXT atID ELSE EXIT
         READ RECORD FROM ONIGHT.TRAN, atID ELSE CONTINUE
         IF DBG THEN
            IF INF.LOGGING THEN
               LOG.MSG = "   .) loading ":atID
               CALL uLOGGER(1, LOG.KEY:LOG.MSG)
            END
         END
         GOSUB BUILD..PAYLOAD
         OTRX +=1
      REPEAT
      IF INF.LOGGING THEN
         LOG.MSG = "   .) ":OTRX:" records from OVERNIGHT.TRAN loaded."
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      * --------------- [ CDC.ACCOUNT ] ------------------
      IF INF.LOGGING THEN
         LOG.MSG = "   .) looking for records in CDC.ACCOUNT & PSEUDO.TRAN"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      PENDING = 1
      READV PSLIST FROM CDC.ACCOUNT, ACID, 15 ELSE PSLIST = ""
      EOPS = DCOUNT(PSLIST, @VM)
      FOR PS = 1 TO EOPS
         READV RECORD FROM PSEUDO.TRAN, PSLIST<1, PS>, 1 ELSE CONTINUE
         RECORD<1,2,6> = PSLIST<1, PS>
         IF DBG THEN
            IF INF.LOGGING THEN
               LOG.MSG = "   .) loading ":PSLIST<1, PS>
               CALL uLOGGER(1, LOG.KEY:LOG.MSG)
            END
         END
         GOSUB BUILD..PAYLOAD
      NEXT PS
      IF INF.LOGGING THEN
         LOG.MSG = "   .) ":EOPS:" records from PSEUDO.TRAN loaded."
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      PENDING = 0
      * --------------------------------------------------
      TODAY = DATE()
      NOW   = INT(TIME())
      ERR   = "" ; DATETIME = TODAY:"_":NOW 
      OPER  = "ADD" ; INTERVAL = "10" ; PERIOD = "m" ; RESULT = ""
      CALL SR.DTMATH (ERR, DATETIME, OPER, INTERVAL, PERIOD, RESULT)
      EXPIRE= FIELD(RESULT, "_", 2)
      IF EXPIRE = "" THEN EXPIRE = NOW + 600
      * --------------------------------------------------
      READU CDR.CTL FROM SL, TODAY ELSE CDR.CTL = ""
      LOCATE(CORREL, CDR.CTL, 1; FND) ELSE FND = -1
      CDR.CTL<1, FND> = CORREL
      CDR.CTL<2, FND> = EXPIRE
      WRITE CDR.CTL ON SL, TODAY
      RELEASE SL, TODAY
      * --------------------------------------------------
      OPENSEQ SLISTS, CORREL TO SEQIO THEN
         WRITESEQ PAYLOAD ON SEQIO ELSE ERR = "WRITESEQ ERROR"
      END
      CLOSESEQ SEQIO
      RECCNT  = DCOUNT(PAYLOAD, @FM)
      PAYLOAD = RECCNT
      IF INF.LOGGING THEN
         LOG.MSG = "   .) ":RECCNT:" transactions have been loaded."
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      * --------------------------------------------------------
END..SRTN:
      IF ERR # "" AND INF.LOGGING THEN
         LOG.MSG = "   .) ERROR: ":PROG:ERR
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      IF INF.LOGGING THEN
         LOG.MSG = "Finished extracts on ":ACCOUNTID:" ------------------------------"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
         LOG.MSG = "   ."
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      RETURN
      * --------------------------------------------------------
BUILD..PAYLOAD:
      HOLD.PAYLOAD = PAYLOAD
      ATR = 1
      STX = 1
      IF A.ARR<1> = 0 THEN 
         PREFIX = atID:MARKER 
         STX = 2
      END ELSE 
         PREFIX = ""
      END
      EOP = DCOUNT(A.ARR, @FM)
***   PAYLOAD := PREFIX
      UPDATES.LOADED = 0
      EOA = DCOUNT(RECORD, @FM)
      FOR AV = ATR TO EOA
         IF PENDING THEN RECORD<AV, 5, 1> = "PENDING" ELSE RECORD<AV, 5, 1> = "POSTED"
         THIS.LINE = PREFIX
         T.MARK = ""
         FOR X = STX TO EOP
            MV = M.ARR<X>
            SV = S.ARR<X>
            CV = C.ARR<X>
            DC = R.ARR<X>                 ;* Date Check field 1/0
            DATUM = RECORD<AV,MV,SV>
            IF DC AND DATUM < LOWDATE THEN 
               THIS.LINE=""
               EXIT
            END
            VAL = OCONV(DATUM, CV)
            THIS.LINE := T.MARK:VAL
            T.MARK = MARKER
         NEXT X
         IF THIS.LINE # "" THEN 
            UPDATES.LOADED = 1
            PAYLOAD := THIS.LINE:@FM
            IF INF.LOGGING THEN
               LOG.MSG = THIS.LINE
               CALL uLOGGER(1, LOG.KEY:LOG.MSG)
            END
         END
      NEXT AV
      IF NOT(UPDATES.LOADED) THEN PAYLOAD = HOLD.PAYLOAD
      HOLD.PAYLOAD = ""
      RETURN
      * --------------------------------------------------------
I.WANT:
      DSD = EREPLACE(DSD, "<fm>", @FM)
      A.ARR = ""
      M.ARR = ""
      S.ARR = ""
      C.ARR = ""
      L.ARR = ""  ;* loop on AMS
      R.ARR = ""  ;* Date range check 'this' field
      CMA   = ","
      INS = 1
      EOI = DCOUNT(DSD, @FM)
      FOR I = 1 TO EOI
         LINE  = EREPLACE(DSD<I>, CMA , @FM)
         IF LINE = "" THEN CONTINUE
         TG    = TRIM(LINE<1>)
         AV    = TRIM(LINE<2>)
         MV    = TRIM(LINE<3>)
         SV    = TRIM(LINE<4>)
         CV    = LINE<5>
         LP    = ""              ;* loop flag AMS
         DC    = 0               ;* date check flag 1/0
         IF TG # "" THEN 
            IF TG = ">" THEN DC = 1 ELSE CONTINUE
         END
         IF INDEX(UPCASE(AV), "N", 1) THEN 
            LP := "A"
            AV = UPCASE(AV)
            AV = EREPLACE(AV, "N", "")
            AV = EREPLACE(AV, "-", "")
            IF NOT(NUM(AV)) OR AV = "" THEN AV = 1
         END
         IF INDEX(UPCASE(MV), "N", 1) THEN 
            LP := "M"
            MV = UPCASE(MV)
            MV = EREPLACE(MV, "N", "")
            MV = EREPLACE(MV, "-", "")
            IF NOT(NUM(MV)) OR MV = "" THEN MV = 1
         END
         IF INDEX(UPCASE(SV), "N", 1) THEN 
            LP := "S"
            SV = UPCASE(SV)
            SV = EREPLACE(SV, "N", "")
            SV = EREPLACE(SV, "-", "")
            IF NOT(NUM(SV)) OR SV = "" THEN SV = 1
         END
         *
         A.ARR<INS> = AV
         M.ARR<INS> = MV
         S.ARR<INS> = SV
         C.ARR<INS> = CV
         L.ARR<INS> = LP
         R.ARR<INS> = DC
         INS += 1
      NEXT I
      RETURN
   END
