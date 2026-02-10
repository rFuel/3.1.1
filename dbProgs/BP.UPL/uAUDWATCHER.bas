      PROMPT ""
$INCLUDE I_Prologue
      * ------------------------------------------------------------ *
      *  1. Produce Audit Log events                                 *
      *  2. Produce uvaudd.log events                                *
      *  3. Produce uvaudd.errlog events                             *
      * ------------------------------------------------------------ *
      *(1) UV audit Logging is controlled through the X-Admin UX/UI  *
      *  In UniLibre's UV dev server, logs are storted in sequential *
      *  files in the /usr/uv/audit/seqlogs directory.               *
      *  uAUDWATCHER reads from these log files                      *
      *     + uvaudd.log & uvaudd.errlog for database events.        *
      *     Then writes to AUDLOG file (type 1 or 19)                *
      *  kProducer   :  reads from AUDLOG and writes to kafka.       *
      *  itsmConsumer:  reads from kafka and calls ServiceNow api    *
      *                                                              *
      *(2) UV audits its own health via the uvaudd daemon. This      *
      *  daemon is started by the uvsmm process and the uvssm process*
      *  monitors the daemon and generates log events on hanging or  *
      *  abnormal stops.                                             *
      *(3) Same as (2)                                               *
      * ------------------------------------------------------------ *
      MEMORY.VARS(1) = "audlog"
      LOG.KEY = MEMORY.VARS(1):@FM
      PROG = "uAUDWATCHER"
      STREAM.FILE = "uSTREAM.LOG"
      *
      ERR = ""
      CALL SR.FILE.OPEN (ERR, "VOC", VOC); IF ERR THEN STOP
      CALL SR.FILE.OPEN (ERR, "BP.UPL", BP.UPL); IF ERR THEN STOP
      CALL SR.OPEN.CREATE (ERR, STREAM.FILE, "DYNAMIC", PROCESSED); IF ERR THEN STOP
      *
      READ PROPS FROM BP.UPL, "properties" ELSE PROPS = ""
      EOI = DCOUNT(PROPS, @FM)
      * AFL - Audit File Location         *
      *       must be a *nix DIRECTORY    *
      AFL = ""
      FOR I = 1 TO EOI
         KEY = FIELD(PROPS<I>, "=", 1)
         IF UPCASE(KEY) = "UVAUDLOG" THEN AFL = FIELD(PROPS<I>, "=", 2); EXIT
      NEXT I
      IF AFL = "" THEN 
         CRT PROG:": UVAUDLOG is undefined"
         STOP
      END
      *
      READ REGISTER FROM BP.UPL, "register" ELSE REGISTER = ""
      *
      WRITE "F":@FM:AFL:@FM:"D_VOC" ON VOC, "AUDLOG"
      OPEN "AUDLOG" TO AUDLOG ELSE
         CRT "---------------------------------------------"
         CRT "Audit File Location: [":AFL:"]"
         CRT "ABORT: cannot access Audit File;"
         CRT "---------------------------------------------"
         CRT "  1. Check permissions are (rwxr-x---)"
         CRT "  2. Check owner=root and group=uvadm"
         CRT "  2. Are you logged in and running as uvadm?"
         STOP
      END
      CLOSE AUDLOG
      atFM = "<fm>"
      atVM = "<vm>"
      atSV = "<sm>"
      WRI  = "WRITE"
      EXE  = "SSELECT AUDLOG BY.DSND @ID"
      DOT  = ">"
      QPTH = "F":@FM:@FM:"D_VOC"
      QPTR = "Q":@FM:@FM
      SLASH= "/"
      QFL  = "qAUDPTR"
      EXEMPT = " BP.UPL OBJ.UPL uDELTA.LOG uLOG ":STREAM.FILE:" VOC &SAVEDLISTS& "
      LOG.MSG = STR(" ",20)
      CALL uLOGGER(0, LOG.KEY:LOG.MSG)
      LOG.MSG = PROG:" ":STR("-",60)
      CALL uLOGGER(0, LOG.KEY:LOG.MSG)
      CRT
      CRT "-------------------------------------------------------"
      CRT "All output is logged to uLOG ":MEMORY.VARS(1)
      CRT
      CRT
      PROG = ""
      ERR  = 0
      LASTCNT = 0
      DONE = ""
      LAST.FILE = ""
      LFLINES = ""
      CHECKED = ""
      IGNORE  = ""
      LOOP
         EXECUTE EXE CAPTURING JUNK
         LOOP
            READNEXT LOG.FILE ELSE EXIT
            LOCATE(LOG.FILE, DONE; lfPOS) THEN 
               LASTCNT = LFLINES<lfPOS>+0
               EOI = DCOUNT(DONE, @FM)
               IF lfPOS # EOI THEN CONTINUE
               *
               IF (LOG.FILE # LAST.FILE) THEN 
                  LOG.MSG = "<heartbeat> waiting for deltas on ":LOG.FILE
                  CALL uLOGGER(0, LOG.KEY:LOG.MSG)
                  LAST.FILE = LOG.FILE
               END
            END ELSE
               LASTCNT = 0
               LOG.MSG = " processing [":LOG.FILE:"] from [AUDLOG]."
               CALL uLOGGER(0, LOG.KEY:LOG.MSG)
            END
            *
            IF FILEINFO(AUDLOG,0) THEN CLOSE AUDLOG
            DONE<-1> = LOG.FILE
            OPENSEQ "AUDLOG",LOG.FILE TO AUDLOG ELSE 
               LOG.MSG = "cannot access [":LOG.FILE
               LOG.MSG:= "] from [AUDLOG] logfile skipped."
               CALL uLOGGER(0, LOG.KEY:LOG.MSG)
               CONTINUE
            END
            TCNT = 0
            LOOP
               TCNT+=1
               READSEQ TREC FROM AUDLOG ELSE EXIT
               IF TCNT < LASTCNT THEN CONTINUE
               *
               DTS = TREC<1>; oDTS = DTS
               EVT = TREC<2>; oEVT = EVT
               ACT = TREC<5>; oACT = ACT
               FIL = TREC<8>; oFIL = FIL
               IID = TREC<9>; oIID = IID
               IF INDEX(FIL, ACT, 1) THEN 
                  FIL = EREPLACE(FIL, ACT, "")
                  IF FIL[1,1] = SLASH THEN FIL = FIL[2,LEN(FIL)]
               END
               REASON = ""
               CHK = EREPLACE(FIL, "D_", "")
               IF (INDEX(EXEMPT,CHK, 1))     THEN REASON:= "exempt file "
               IF NOT(INDEX(EVT, WRI, 1))    THEN REASON:= "bad evt "
               IF IID = ""                   THEN REASON:= "no iid "
               *
               cFIL = FIL
               IF INDEX(cFIL, SLASH, 1) THEN cFIL = FIELD(cFIL, SLASH, 1)
               IF TRIM(cFIL) = "" THEN CONTINUE
               cACT = ACT
               IF INDEX(cACT, SLASH, 1) THEN cACT = FIELD(cACT, SLASH, DCOUNT(cACT, SLASH))
               IF TRIM(cACT) = "" THEN CONTINUE
               *
               EVENT.SOURCE = cACT:" ":cFIL
               LOCATE(EVENT.SOURCE, IGNORE; igPOS) THEN CONTINUE
               *
               LOCATE(EVENT.SOURCE, REGISTER; rfPOS) ELSE REASON:= "file not registered for streams "
               IF REASON # ""                THEN CONTINUE
               IF INDEX(FIL, SLASH, 1) THEN
                  DIR.PARTS = DCOUNT(FIL, SLASH)
                  TMP = FIL
                  FIL = FIELD(TMP, SLASH, DIR.PARTS)
                  DIR.PARTS -= 1
                  ACT = ""
                  FOR I = 1 TO DIR.PARTS
                     ACT := SLASH:FIELD(TMP, SLASH, I)
                  NEXT I
                  ACT = ACT[2,LEN(ACT)]
               END
               KEY = DTS:DOT:ACT:DOT:FIL:DOT:IID
               READ CHK FROM PROCESSED, KEY  THEN CONTINUE
               *
               REC = ""
               IF INDEX(FIL, SLASH, 1) THEN
                  REC = QPTH
                  REC<2> = FIL
                  DIR.PARTS = DCOUNT(FIL, SLASH)
                  TMP = FIL
                  FIL = FIELD(TMP, SLASH, DIR.PARTS)
                  DIR.PARTS -= 1
                  ACT = ""
                  FOR I = 1 TO DIR.PARTS
                     ACT := SLASH:FIELD(TMP, SLASH, I)
                  NEXT I
                  ACT = ACT[2,LEN(ACT)]
               END ELSE
                  IF (INDEX(ACT, SLASH, 1)) THEN
                     REC = QPTH
                     REC<2> = ACT:SLASH:FIL
                  END ELSE
                     REC = QPTR
                     REC<2> = FIL
                     REC<3> = "D_":FIL
                  END
               END
               IF REC = "" THEN 
                  CRT "ERROR: cannot understand file and accout"
                  CRT "       File: [":FIL:"]"
                  CRT "    Account: [":ACT:"]"
                  CRT "   Log File: [":LOG.FILE:"]"
                  CRT "Transaction: [":TREC:"]"
                  STOP
               END
               WRITE REC ON VOC, QFL
               *
               OPEN QFL TO QFILE ELSE
                  LOG.MSG = "cannot access [":FIL:"] in [":ACT
                  LOG.MSG:= "] event ignored."
                  CALL uLOGGER(0, LOG.KEY:LOG.MSG)
                  IGNORE<-1> = EVENT.SOURCE
                  CONTINUE
               END
               READ RECORD FROM QFILE, IID ELSE
                  LOG.MSG = "cannot read [":IID:"] from [":FIL:"] in [":ACT
                  LOG.MSG:= "] event ignored."
                  CALL uLOGGER(0, LOG.KEY:LOG.MSG)
                  CONTINUE
               END
               LOG.MSG = "logging [":IID:"] from [":FIL:"] in [":ACT
               LOG.MSG:= "]."
               CALL uLOGGER(0, LOG.KEY:LOG.MSG)
               CALL uAES(ERR, DTS, ACT, FIL, IID, RECORD)
               IF ERR THEN STOP
               pREC = TIMEDATE():@FM
               pREC:= oDTS:@FM
               pREC:= oEVT:@FM
               pREC:= oACT:@FM
               pREC:= oFIL:@FM
               pREC:= oIID:@FM
               WRITE pREC ON PROCESSED, KEY
               RQM
            REPEAT
            CLOSE AUDLOG
            IF LFLINES<lfPOS> < TCNT THEN LFLINES<lfPOS> = TCNT
            RQM; RQM
         REPEAT
         RQM; RQM
      REPEAT
      STOP
   END
