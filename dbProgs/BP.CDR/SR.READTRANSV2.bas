      SUBROUTINE SR.READTRANSV2 (ERR, DSD, LOWDATE, ACCOUNTID, CORREL, PAYLOAD)
$INCLUDE I_Prologue
      *
      * ----------------------------------------------------------------------
      *
      PROG     = "SR.READTRANS"
      LOG.KEY  = "CDR-OB":@FM
      ACCTID   = ACCOUNTID
      ERR      = "Unknown Error"
      PAYLOAD  = ""
      MARKER   = "<tm>"
      T.MARK   = ""
      F.MARK   = "<fm>"
      SLISTS   = "&SAVEDLISTS&"
      LII      = ""
      CLI      = ""
      atID     = ""
      STAR     = "*"
      DBG.SW   = 0
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
         LOG.MSG = "Start SR.READTRANSv2 for ":ACCOUNTID:" --------------------------"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      *
      ACCTID= atID
      LASTLN= 1
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
      CALL SR.FILE.OPEN (ERR, "BP.UPL"       , BP.UPL       )         ; IF ERR # "" THEN ERR = " 0 ":ERR ; GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "TRAN"         , TRAN         )         ; IF ERR # "" THEN ERR = " 1 ":ERR ; GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "TRAN.EXT"     , TRANEXT      )         ; IF ERR # "" THEN ERR = " 2 ":ERR ; GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "CDC.ACCOUNT"  , CDC.ACCOUNT  )         ; IF ERR # "" THEN ERR = " 3 ":ERR ; GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "PSEUDO.TRAN"  , PSEUDO.TRAN  )         ; IF ERR # "" THEN ERR = " 4 ":ERR ; GO END..SRTN
      CALL SR.FILE.OPEN (ERR, SLISTS         , SL           )         ; IF ERR # "" THEN ERR = " 5 ":ERR ; GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "CDR.WORKFILE" , CDR.WORKFILE )         ; IF ERR # "" THEN ERR = " 6 ":ERR ; GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "BPAY.BILLER.NAME" , BPAY.BILLER.NAME ) ; IF ERR # "" THEN ERR = " 7 ":ERR ; GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "OVERNIGHT.TRAN", ONIGHT.TRAN )         ; IF ERR # "" THEN ERR = " 8 ":ERR ; GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "BPAY.BILLER.NAME" , BPAY.BILLER.NAME ) ; IF ERR # "" THEN ERR = " 9 ":ERR ; GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "NPP.PAYMENT.OUT"  , NPP.PAYMENT.OUT )  ; IF ERR # "" THEN ERR = " 10":ERR ; GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "NPP.PAYMENT.IN"   , NPP.PAYMENT.IN  )  ; IF ERR # "" THEN ERR = " 11":ERR ; GO END..SRTN
      *
      READ CONTROL FROM BP.UPL, "properties" ELSE CONTROL = ""
      DPOS = INDEX(UPCASE(CONTROL), "DEBUG=", 1)
      IF DPOS THEN
         ANS = FIELD(CONTROL[DPOS, 99]<1>, "=", 2)
         IF ANS = 1 OR UPCASE(ANS) = "TRUE" THEN DBG.SW = 1
      END
      *
      * --------------------------------------------------
      *     SANITY check before checking Correlation ID  *
      * --------------------------------------------------
      *
      ERR = ""
      READ RECORD FROM TRAN, atID ELSE 
         ERR = "No such item on TRAN"
         GO END..SRTN
      END
      * --------------------------------------------------
      * If it has been built, DO NOT build it again     !!
      * Other process will delete them after N minutes  !!
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
      *
      GOSUB I.WANT
      HIGHDATE = ""
      IF INDEX(LOWDATE, "-", 1) > 0 THEN
         HIGHDATE= FIELD(LOWDATE, "-", 2)
         LOWDATE = FIELD(LOWDATE, "-", 1)
      END
      *
      * -------------------- [ TRAN ] --------------------
      PENDING = 0
      FOR I = 1 TO 16
         RECORD = DELETE(RECORD, 1, 0, 0)
      NEXT I
      EOX = 0
      GOSUB LOAD..TRANS
      IF INF.LOGGING THEN
         LOG.MSG = "   .) transactions from TRAN [":EOX:"]"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      * ------------------ [ TRAN.EXT ] ------------------
      ACID = atID
      atID = ACID:"/0"
      EOX = 0
      READ INDEX.REC FROM TRANEXT, atID ELSE INDEX.REC = ""
      EOE = DCOUNT(INDEX.REC, @FM)
      FOR EXT.ID = 1 TO EOE 
         IDX.DTE = INDEX.REC<EXT.ID, 1>
         IF IDX.DTE # "" AND IDX.DTE < LOWDATE THEN CONTINUE
         atID = ACID:"/":EXT.ID
         READ RECORD FROM TRANEXT, atID ELSE CONTINUE
         IF DBG.SW THEN
            IF DBG.SW AND INF.LOGGING THEN
               LOG.MSG = "      .> loading ":atID
               CALL uLOGGER(1, LOG.KEY:LOG.MSG)
            END
         END
         GOSUB LOAD..TRANS
      NEXT EXT.ID
      IF INF.LOGGING THEN
         LOG.MSG = "   .) transactions from TRAN.EXT [":EOX:"]"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      WRITE "" ON SL, CORREL
      * --------------- [ OVERNIGHT.TRAN ] ---------------
      EOX = 0
      READ RECORD FROM ONIGHT.TRAN, ACCTID THEN
         IF DBG.SW AND INF.LOGGING THEN
            LOG.MSG = "   .) loading ":atID
            CALL uLOGGER(1, LOG.KEY:LOG.MSG)
         END
***      GOSUB BUILD..PAYLOAD
         GOSUB LOAD..TRANS
      END ELSE
         IF DBG.SW AND INF.LOGGING THEN
            LOG.MSG = "   .) Nothing for ":atID
            CALL uLOGGER(1, LOG.KEY:LOG.MSG)
         END
      END
      IF INF.LOGGING THEN
         LOG.MSG = "   .) transactions from OVERNIGHT.TRAN [":EOX:"]"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      *
      * ---------------- [ PSEUDO.TRAN ] -----------------
      EOX = 0
      PENDING = 1
      READV PSLIST FROM CDC.ACCOUNT, ACID, 15 ELSE PSLIST = ""
      EOPS = DCOUNT(PSLIST, @VM)
      FOR PS = 1 TO EOPS
         READV RECORD FROM PSEUDO.TRAN, PSLIST<1, PS>, 1 ELSE CONTINUE
         RECORD<1,2,6> = PSLIST<1, PS>
         IF DBG.SW THEN
            IF DBG.SW AND INF.LOGGING THEN
               LOG.MSG = "   .) loading ":PSLIST<1, PS>
               CALL uLOGGER(1, LOG.KEY:LOG.MSG)
            END
         END
         GOSUB LOAD..TRANS
      NEXT PS
      IF INF.LOGGING THEN
         LOG.MSG = "   .) transactions from PSEUDO.TRAN [":EOX:"]"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      PENDING = 0
      * --------------------------------------------------
      GOSUB FIX..TRANS
      * --------------------------------------------------
      IF INF.LOGGING THEN
         LOG.MSG = "   .) Stitching all together from CDR.WORKFILE"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
$IFDEF isRT
      EXE = 'SSELECT CDR.WORKFILE = "':ACCTID:']" '
      EXE:= 'BY-DSND A2 '
      EXE:= 'BY-DSND GRP2 '
      EXECUTE EXE RTNLIST ACTIVE.LIST CAPTURING OUTPUT
$ELSE
      EXE = "SSELECT CDR.WORKFILE LIKE ":ACCTID:"... "
      EXE:= 'BY.DSND EVAL "@RECORD<2,1,1>" FMT "10R" '
      EXE:= 'BY.DSND EVAL "FIELD(@ID, ':"'*'":', 2)" FMT ':'"10R"'
      EXECUTE EXE CAPTURING OUTPUT
$ENDIF
      IF INF.LOGGING THEN
         LOG.MSG = "   .) ":EXE
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      SEL.CNT = 0
      * --------------------------------------------------
      LOOP
$IFDEF isRT
         READNEXT ID FROM ACTIVE.LIST ELSE EXIT
$ELSE
         READNEXT ID ELSE EXIT
$ENDIF
         SEL.CNT += 1
         READ RECORD FROM CDR.WORKFILE, ID ELSE CONTINUE
         GOSUB BUILD..PAYLOAD
      REPEAT
      *
      RECCNT  = DCOUNT(PAYLOAD, @FM)
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
      IF PAYLOAD<RECCNT> = "" AND RECCNT > 0 THEN RECCNT -= 1
      PAYLOAD = RECCNT
      IF INF.LOGGING THEN
         LOG.MSG = "   .) ----------------------------------------------"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
         LOG.MSG = "   .) ":SEL.CNT:" transactions sent for processing."
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
         LOG.MSG = "   .) ":RECCNT:" transaction(s) loaded."
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
         LOG.MSG = "   .) ----------------------------------------------"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      * --------------------------------------------------------
END..SRTN:
      IF ERR # "" AND INF.LOGGING THEN
         LOG.MSG = "   .) ERROR: ":PROG:ERR
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      *
      * --------------------------------------------------------
$IFDEF isRT
      EXE = 'SELECT CDR.WORKFILE = "':ACCTID:']"'
$ELSE
      EXE = "SSELECT CDR.WORKFILE LIKE ":ACCTID:"... "
$ENDIF
      IF INF.LOGGING THEN
         LOG.MSG = "   .) Cleanup CDR.WORKFILE"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
         LOG.MSG = "   .) ":EXE
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      EXECUTE EXE RTNLIST DEL.LIST CAPTURING JUNK
      DELCNT=0
      LOOP
         READNEXT ID FROM DEL.LIST ELSE EXIT
         DELCNT+=1
         DELETE CDR.WORKFILE, ID
      REPEAT
      * --------------------------------------------------------
      *
      IF INF.LOGGING THEN
         LOG.MSG = "   .) ":DELCNT:" CDR.WORKFILE items deleted."
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
         LOG.MSG = "   .) Data moved into SL ":CORREL
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
         LOG.MSG = "Finished extracts on ":ACCOUNTID:" ------------------------------"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
         LOG.MSG = " ***********************************************************************"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
         LOG.MSG = ""
      END
      RETURN
      *
      * --------------------------------------------------------
      *            E N D   O F   S U B R O U T I N E           *
      * --------------------------------------------------------
LOAD..TRANS:
      EOX = DCOUNT(RECORD, @FM)
      FOR X = 1 TO EOX
         LINE = RECORD<X>
         TRXID= RECORD<X,2,6>    ; * this    TranId
         PPTID= RECORD<X,2,8>    ; * parent  TranId
         *
         SORTD = RECORD<X,2,2>                      ; * Effective Date   
         IF SORTD ="" THEN SORTD = RECORD<X,2,1>    ; * Process   Date   
         IF SORTD ="" THEN SORTD = DATE()           ; * Today - or else we lose it ?????
         *
***      IF HIGHDATE # "" AND SORTD > HIGHDATE THEN CONTINUE
***      *
         IF PENDING THEN RECORD<X, 5, 1> = "PENDING" ELSE RECORD<X, 5, 1> = "POSTED"
         BILLERCODE = RECORD<X,4,7>
         BILLERNAME = ""
         IF BILLERCODE # "" THEN
            BILLERCODE = ("000000000000":BILLERCODE) "R#10"
            READV BILLERNAME FROM BPAY.BILLER.NAME, BILLERCODE, 2 ELSE BILLERNAME = ""
         END
         RECORD<X, 5, 2> = BILLERNAME
         *
         IF PPTID # "" THEN
            KEY = ACCTID:STAR:PPTID
            READ TREC FROM CDR.WORKFILE, KEY THEN
               NARR = RECORD<X,4,1>
               TREC<1,4,1> := " ":NARR
               IF PENDING THEN TREC<X, 5, 1> = "PENDING" ELSE TREC<X, 5, 1> = "POSTED"
               WRITE TREC ON CDR.WORKFILE, KEY
            END ELSE 
               * new parent ?? this is an error.
               * record the error & fix later.
               KEY = ACCTID:STAR:PPTID:STAR:TRXID
               WRITE "" ON CDR.WORKFILE, KEY
               * remember the child
               KEY = ACCTID:STAR:TRXID
               WRITE RECORD<X> ON CDR.WORKFILE, KEY
            END
         END ELSE
            KEY = ACCTID:STAR:TRXID
            ITEM= RECORD<X>:@FM:SORTD
            WRITE ITEM ON CDR.WORKFILE, KEY
         END
      NEXT X
      RETURN
      * --------------------------------------------------------
FIX..TRANS:
$IFDEF isRT
      EXE = 'SELECT CDR.WORKFILE WITH GRP3 # ""'
      EXECUTE EXE RTNLIST ACTIVE.LIST CAPTURING JUNK
$ELSE
      EXE = "SELECT CDR.WORKFILE LIKE ...*...*..."
      EXECUTE EXE CAPTURING JUNK
$ENDIF
      NBR.FIXED=0
      LOOP
$IFDEF isRT
         READNEXT OLD.ID FROM ACTIVE.LIST ELSE EXIT
$ELSE
         READNEXT OLD.ID ELSE EXIT
$ENDIF
         READ TREC FROM CDR.WORKFILE, OLD.ID ELSE
            IF INF.LOGGING THEN
               LOG.MSG = "      .)  Record missing: ":OLD.ID
               CALL uLOGGER(1, LOG.KEY:LOG.MSG)
            END
            DELETE CDR.WORKFILE, OLD.ID
            CONTINUE
         END
         P.KEY = FIELD(OLD.ID, STAR, 1):STAR:FIELD(OLD.ID, STAR, 2)
         C.KEY = FIELD(OLD.ID, STAR, 1):STAR:FIELD(OLD.ID, STAR, 3)
         READ CHILD FROM CDR.WORKFILE, C.KEY ELSE 
            IF INF.LOGGING THEN
               LOG.MSG = "      .)  Child record missing: ":C.KEY
               CALL uLOGGER(1, LOG.KEY:LOG.MSG)
            END
            DELETE CDR.WORKFILE, OLD.ID
            CONTINUE
         END
         NARR = CHILD<1,4,1>
         READ PARENT FROM CDR.WORKFILE, P.KEY ELSE 
            IF INF.LOGGING THEN
               LOG.MSG = "      .)  Parent record missing: ":P.KEY
               CALL uLOGGER(1, LOG.KEY:LOG.MSG)
            END
            DELETE CDR.WORKFILE, OLD.ID
            CONTINUE
         END
         ORIG = PARENT<1,4,1>
         PARENT<1,4,1> := " ":NARR
         WRITE PARENT ON CDR.WORKFILE, P.KEY
         DELETE CDR.WORKFILE, OLD.ID
         DELETE CDR.WORKFILE, C.KEY
         IF INF.LOGGING THEN
            LOG.MSG = "      .)  Child: ":C.KEY:"   ":NARR
            CALL uLOGGER(1, LOG.KEY:LOG.MSG)
            LOG.MSG = "      .) Parent: ":P.KEY:"   ":ORIG
            CALL uLOGGER(1, LOG.KEY:LOG.MSG)
            LOG.MSG = "      .) Update: ":P.KEY:"   ":PARENT<1,4,1>
            CALL uLOGGER(1, LOG.KEY:LOG.MSG)
         END
         NBR.FIXED += 1
      REPEAT
      IF INF.LOGGING AND NBR.FIXED>0 THEN
         LOG.MSG = "   .) Fixed: ":NBR.FIXED:" broken parent-child tran(s)."
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      RETURN
      * --------------------------------------------------------
BUILD..PAYLOAD:
      HOLD.PAYLOAD = PAYLOAD
      THIS.TRXID = FIELD(ID,'*', 2)
      ATR = 1
      STX = 1
      IF A.ARR<1> = 0 THEN 
         PREFIX = atID:MARKER 
         STX = 2
      END ELSE 
         PREFIX = ""
      END
      EOP = DCOUNT(A.ARR, @FM)
      UPDATES.LOADED = 0
      EOA = DCOUNT(RECORD, @FM)
      FOR AV = ATR TO EOA
         THIS.LINE = PREFIX
         OUT.OF.LO.RANGE = 1
         OUT.OF.HI.RANGE = 1
         T.MARK = ""
         FOR X = STX TO EOP
            MV = M.ARR<X>
            SV = S.ARR<X>
            CV = C.ARR<X>
            DC = R.ARR<X>                 ; * Date Check field 1/0
            IF MV="" OR MV=0 THEN MV = 1
            IF SV="" OR SV=0 THEN SV = 1
            DATUM = RECORD<AV,MV,SV>
***         IF DC AND OUT.OF.RANGE THEN
***            IF DATUM < LOWDATE THEN THIS.LINE="" ; EXIT
***            IF HIGHDATE#"" AND DATUM > HIGHDATE THEN  THIS.LINE="" ; EXIT
***         END
            *
            * ---------------------------------------------------------------------
            *
            IF DC AND DATUM # "" THEN
               IF OUT.OF.LO.RANGE THEN
                  IF DATUM >= LOWDATE THEN 
                     OUT.OF.LO.RANGE = 0
                  END ELSE 
                     OUT.OF.LO.RANGE = 1
                  END
               END
               IF OUT.OF.HI.RANGE AND HIGHDATE#"" THEN
                  IF DATUM <= HIGHDATE THEN 
                     OUT.OF.HI.RANGE = 0 
                  END ELSE 
                     OUT.OF.HI.RANGE = 1
                  END
               END
            END
            IF CV # "" THEN 
               IF CV[1,2] = "MT" THEN DATUM = INT(DATUM / 1000)
               VAL = OCONV(DATUM, CV) 
            END ELSE 
               VAL = DATUM
            END
            THIS.LINE := T.MARK:VAL
            T.MARK = MARKER
         NEXT X
         *
         OUT.OF.RANGE = OUT.OF.LO.RANGE 
         IF HIGHDATE # "" THEN OUT.OF.RANGE = OUT.OF.RANGE OR OUT.OF.HI.RANGE
         IF OUT.OF.RANGE THEN 
            THIS.LINE = ""
         END ELSE
            IF DBG.SW THEN
               LOG.MSG = "       .) TrxID ":THIS.TRXID:" included"
               CALL uLOGGER(1, LOG.KEY:LOG.MSG)
            END
         END
         *
         IF THIS.LINE # "" THEN 
            * ----------- [ Link NPP In / Out Data ] -------------
            NPPOUT.KEY = RECORD<AV,31,1>
            NPPIN..KEY = RECORD<AV,31,2>
            READ NPPOUT.REC FROM NPP.PAYMENT.OUT, NPPOUT.KEY ELSE NPPOUT.REC = ""
            READ NPPIN..REC FROM NPP.PAYMENT.IN , NPPIN..KEY ELSE NPPIN..REC = ""
            * NPP Out
            NPPO.PAYEE = NPPOUT.REC<27,1,1>
            IF NPPO.PAYEE = "" THEN NPPO.PAYEE = NPPOUT.REC<31,1,1>
            NPPO.PAYER = ""
            NPPO.E2EID = NPPOUT.REC<6,1,1>
            NPPO.EDESC = NPPOUT.REC<32,1,1>
            * NPP In 
            NPPI.PAYEE = NPPIN..REC<27,1,1>
            IF NPPI.PAYEE = "" THEN NPPI.PAYEE = NPPIN..REC<31,1,1>
            NPPI.PAYER = NPPIN..REC<18,1,1>
            NPPI.E2EID = NPPIN..REC<6,1,1>
            NPPI.EDESC = NPPIN..REC<32,1,1>
            *
            THIS.LINE := MARKER:NPPO.PAYEE:MARKER:NPPO.PAYER:MARKER:NPPO.E2EID:MARKER:NPPO.EDESC
            THIS.LINE := MARKER:NPPI.PAYEE:MARKER:NPPI.PAYER:MARKER:NPPI.E2EID:MARKER:NPPI.EDESC
            * ----------------------------------------------------
            UPDATES.LOADED = 1
            PAYLOAD := THIS.LINE:@FM
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
         IF (AV:MV:SV) = "" THEN CONTINUE
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
