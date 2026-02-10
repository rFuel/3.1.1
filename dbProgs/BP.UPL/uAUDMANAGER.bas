      PROMPT ""
$INCLUDE I_Prologue
      MEMORY.VARS(1) = "audman"
      GOSUB INITIALISE
      *
      * -----------------------------------------------------------
      * M A I N      L O O P
      * -----------------------------------------------------------
      ELYPSES   = "..."
      MT        = ""
      DONE      = ""
      DONE.ATR  = ""
      STARTFROM = 0
      FIRST     = 1
      LOOP
         READ STOP.SW FROM BP.UPL, "STOP" ELSE STOP.SW = ""
         IF UPCASE(STOP.SW) = "STOP" THEN EXIT
         *
         READ uvaudd.log FROM uvHOME, "uvaudd.log" ELSE
            LOG.MSG = " FATAL: uvaudd.log is missing."
            CALL uLOGGER(0, LOG.KEY:LOG.MSG) 
            EXIT
         END
         LOGFILE  = ""
         LASTFILE = ""
         EOF = DCOUNT(uvaudd.log, @FM)
         FOR AL = 1 TO EOF
            LINE = uvaudd.log<AL>
            LOG.MSG = " uvaudd.log<":AL:">   ":LINE
            CALL uLOGGER(0, LOG.KEY:LOG.MSG) 
            IF LOWER(FIELD(LINE, "[", 1)) = "buffer" THEN
               IF NOT(INDEX(LINE, AFL, 1)) THEN CONTINUE
               TMP1    = LINE[INDEX(LINE, AFL, 1)+LEN(AFL), 999]
               LOGFILE = EREPLACE(TMP1, ELYPSES, MT)
               IF LOGFILE # LASTFILE THEN
                  LOCATE(LOGFILE, DONE; x) ELSE
                     LOG.MSG = " Checking ":LOGFILE
                     CALL uLOGGER(0, LOG.KEY:LOG.MSG) 
                  END
                  GOSUB PROCESS
                  LASTFILE = LOGFILE
               END ELSE
                  LOG.MSG = "    > ":LOGFILE:"  has been processed: ignoring ..."
                  CALL uLOGGER(0, LOG.KEY:LOG.MSG) 
               END
            END
         NEXT AL
         IF FIRST THEN
            LOG.MSG = " HEARTBEAT: waiting for events on ":LASTFILE
            CALL uLOGGER(0, LOG.KEY:LOG.MSG) 
            FIRST = 0
         END
         RQM; RQM
      REPEAT
      DELETE VOC, "uvHOME"
      DELETE VOC, "uvAUDLOGS"
      CLOSE
      CLEAR
      STOP
      * -----------------------------------------------------------
PROCESS:
      OPENSEQ "uvAUDLOGS", LOGFILE TO AUDLOG ELSE
         LOG.MSG = " FATAL: ":AFL:LOGFILE:" is missing."
         CALL uLOGGER(0, LOG.KEY:LOG.MSG)
         RETURN
      END
      LOCATE(LOGFILE, DONE; lfPOS) THEN
         STARTFROM = DONE.ATR<lfPOS>
      END ELSE
         DONE<-1> = LOGFILE
         STARTFROM = 0
         lfPOS = -1
      END
      FOR ATR = 1 TO STARTFROM
         READSEQ JUNK FROM AUDLOG ELSE
            LOG.MSG = " FATAL: ":AFL:LOGFILE:" sequential read error."
            CALL uLOGGER(0, LOG.KEY:LOG.MSG)
            RETURN
         END
      NEXT ATR
      pREC = ""
      ATR = STARTFROM
      LOOP
         READSEQ TREC FROM AUDLOG ELSE EXIT
         ATR += 1
         DTS = TREC<1>; oDTS = DTS
         EVT = TREC<2>; oEVT = EVT
         ACT = TREC<5>; oACT = ACT
         FIL = TREC<8>; oFIL = FIL
         IID = TREC<9>; oIID = IID
         IF NOT(INDEX(EVT, WRITE.EVT, 1))      THEN CONTINUE
         *
         cFIL = FIL
         IF INDEX(cFIL, SLASH, 1) THEN cFIL = FIELD(cFIL, SLASH, 1)
         IF TRIM(cFIL) = "" THEN CONTINUE
         cACT = ACT
         IF INDEX(cACT, SLASH, 1) THEN cACT = FIELD(cACT, SLASH, DCOUNT(cACT, SLASH))
         IF TRIM(cACT) = "" THEN CONTINUE
         EVENT.SOURCE = cACT:" ":cFIL
         LOCATE(EVENT.SOURCE, REGISTER; rfPOS) ELSE CONTINUE
         *
         IF INDEX(FIL, ACT, 1) THEN 
            FIL = EREPLACE(FIL, ACT, "")
            IF FIL[1,1] = SLASH THEN FIL = FIL[2,LEN(FIL)]
         END
         KEY = DTS
         READ CHK FROM PROCESSED, KEY  THEN CONTINUE
         REC = QPTR
         REC<2> = cACT
         REC<3> = cFIL
         WRITE REC ON VOC, QFL
         *
         IF FILEINFO(QFILE, 0) THEN CLOSE QFILE
         OPEN QFL TO QFILE ELSE
            LOG.MSG = "cannot access [":FIL:"] in [":ACT
            LOG.MSG:= "] event ignored."
            CALL uLOGGER(0, LOG.KEY:LOG.MSG)
            CONTINUE
         END
         READ RECORD FROM QFILE, IID ELSE
            * LOG.MSG = "cannot read [":IID:"] from [":FIL:"] in [":ACT
            * LOG.MSG:= "] event ignored."
            * CALL uLOGGER(0, LOG.KEY:LOG.MSG)
            CONTINUE
         END
         LOG.MSG = "logging [":IID:"] from [":FIL:"] in [":ACT
         LOG.MSG:= "]."
         CALL uLOGGER(0, LOG.KEY:LOG.MSG)
         CALL uAES(ERR, KEY, ACT, FIL, IID, RECORD)
         IF ERR THEN STOP
         WRITE pREC ON PROCESSED, KEY
      REPEAT
      DONE.ATR<lfPOS> = ATR
      RQM
      RETURN
      * -----------------------------------------------------------
INITIALISE:
      LOG.KEY = MEMORY.VARS(1):@FM
      PROG = "uAUDMANAGER"
      STREAM.FILE = "uSTREAM.LOG"
      CALL SR.FILE.OPEN (ERR, "VOC", VOC); IF ERR THEN STOP
      CALL SR.FILE.OPEN (ERR, "BP.UPL", BP.UPL); IF ERR THEN STOP
      CALL SR.OPEN.CREATE (ERR, STREAM.FILE, "DYNAMIC", PROCESSED); IF ERR THEN STOP
      *
      READ PROPS FROM BP.UPL, "properties" ELSE PROPS = ""
      EOI = DCOUNT(PROPS, @FM)
      AFL = ""       ;* AFL - Audit File Location
      ALF = ""       ;* ALF - Audit Log FILE     
      FOR I = 1 TO EOI
         KEY = FIELD(PROPS<I>, "=", 1)
         IF UPCASE(KEY) = "UVAUDLOG" THEN AFL = FIELD(PROPS<I>, "=", 2)
         IF UPCASE(KEY) = "UVHOME"   THEN ALF = FIELD(PROPS<I>, "=", 2)
      NEXT I
      IF AFL = "" THEN CRT PROG:": UVAUDLOG is undefined" ; STOP
      IF ALF = "" THEN CRT PROG:": UVHOME is undefined"; STOP
      *
      READ REGISTER FROM BP.UPL, "register" ELSE REGISTER = ""
      *
      * --------------------------------
      * REGISTER of files to stream     
      * {account} {file}                
      * --------------------------------
      *
      IF TRIM(REGISTER) = "" THEN CRT PROG:": no files are registered"; STOP
      * ----------------------------------------------------------------------------------
      READ CHK FROM VOC, "uvHOME" THEN
         LOG.MSG = PROG:": did not clean up VOC uvHOME - please remove and restart"
         CALL uLOGGER(0, LOG.KEY:LOG.MSG)
         CRT LOG.MSG; STOP
      END
      WRITE "F":@FM:ALF:@FM:"D_VOC" ON VOC, "uvHOME"
      CALL SR.FILE.OPEN (ERR, "uvHOME", uvHOME); IF ERR THEN STOP
      READ CHK FROM uvHOME, "uvaudd.log" ELSE
         CRT "---------------------------------------------"
         CRT "Audit File Location: [":ALF:"]"
         CRT "ABORT: cannot access [":ALF:"uvaudd.log]"
         CRT "---------------------------------------------"
         CRT "  1. Check permissions are (rwxr-x---)"
         CRT "  2. Check owner=uvadm and group=rfuel"
         CRT "  2. Are you logged in and running as uvadm?"
         STOP
      END
      * ----------------------------------------------------------------------------------
      READ CHK FROM VOC, "uvAUDLOGS" THEN
         LOG.MSG = PROG:": did not clean up VOC uvAUDLOGS - please remove and restart"
         CALL uLOGGER(0, LOG.KEY:LOG.MSG)
         CRT LOG.MSG; STOP
      END
      WRITE "F":@FM:AFL:@FM:"D_VOC" ON VOC, "uvAUDLOGS"
      CRT
      CRT "-------------------------------------------------------"
      CRT "All output will now be logged to uLOG ":MEMORY.VARS(1)
      CRT "-------------------------------------------------------"
      CRT
      CRT
      LOG.MSG = (PROG:" ":STR("-",120)) "L#80"
      CALL uLOGGER(0, LOG.KEY:LOG.MSG)
      *
      WRITE.EVT = "WRITE"
      DOT  = ">"
      QPTH = "F":@FM:@FM:"D_VOC"
      QPTR = "Q":@FM:@FM
      SLASH= "/"
      QFL  = "qAUDPTR"
      *
      RETURN
   END

