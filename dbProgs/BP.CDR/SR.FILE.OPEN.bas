      SUBROUTINE SR.FILE.OPEN (ERR, INFILE, HANDLE)
$INCLUDE I_Prologue
      *
      IF INFILE="" THEN ERR="No File name has been provided!"; RETURN
      VFILE = "VOC"
$IFDEF isRT
      IF INFILE = "VOC" THEN INFILE = "MD"
      VFILE = "MD"
$ENDIF
      ERR = ""
      FPOS= 0
      DACCT=""
      LOG.KEY = MEMORY.VARS(1):@FM
      EXECUTE "WHO" CAPTURING JUNK
$IFDEF isRT
      WHOAMI = FIELD(FIELD(FIELD(JUNK, "(", 2), ")", 1), ",", 2)
$ELSE
      WHOAMI = FIELD(JUNK, " ", 2)
$ENDIF
      CALL SR.GET.PROPERTY("dacct", DACCT)
      IF DACCT = "" THEN DACCT = WHOAMI
      CALL SR.GET.PROPERTY("msgid", MSGID)
      IF (MSGID="") THEN 
         MSGID = MEMORY.VARS(200)+1
         MEMORY.VARS(200) = MSGID
      END
      *
      IF INFILE[1,4] = "upl_" AND DCOUNT(INFILE, "_") > 3 THEN
         usefile = FIELD(INFILE, "_", 2)
         useacct = FIELD(INFILE, "_", 3)
         lx      = LEN("upl_":usefile:"_":useacct:"_")
         usemsgid= INFILE[lx+1, LEN(INFILE)]
         zFILE   = "upl_":usefile:"_":useacct
      END ELSE
         usefile = INFILE
         useacct = DACCT
         usemsgid= MSGID
         zFILE   = INFILE
      END
      
      IF zFILE[1,5] = "DICT " THEN 
         uplFILE = zFILE[INDEX(zFILE, " ", 1)+1, LEN(zFILE)] 
      END ELSE 
         uplFILE = zFILE
      END
      fPOS = INDEX(LOCAL.FILES, " ":uplFILE:" ", 1)
$IFDEF isRT
      LOOKIN  = FNAMES
      LOCATE zFILE IN LOOKIN SETTING FPOS ELSE FPOS=1
$ELSE
      LOOKIN  = FNAMES
      LOCATE(zFILE, LOOKIN; FPOS) ELSE FPOS=1
$ENDIF
      FAIL.CNT=0
      REM.ACCESS = 0
TRY..OPEN:
      IF FNAMES<FPOS> = zFILE THEN
         HANDLE = FHANDLES(FPOS)
$IFDEF isRT
         IF HANDLE = "" THEN NOT.OPEN=1 ELSE NOT.OPEN=0
$ELSE
         IF FILEINFO(HANDLE, 0) = 0 THEN NOT.OPEN=1 ELSE NOT.OPEN=0
$ENDIF
         IF NOT.OPEN THEN 
            FNAMES<FPOS> = ""
            FHANDLES(FPOS) = ""
            GO TRY..OPEN
         END
      END ELSE
         * -----------------------------------------------------------------
         IF INFILE[1,4] = "upl_" AND DCOUNT(INFILE, "_") > 3 THEN
            VREC = "Q":@FM:useacct:@FM:usefile
            LOCATE(VFILE, FNAMES; vPOS) THEN
               WRITE VREC ON FHANDLES(vPOS), zFILE
            END
         END
         * -----------------------------------------------------------------
         OPEN zFILE TO HANDLE THEN
$IFDEF isRT
            MAX = 1000
$ELSE
            MAX = INMAT(FHANDLES)<1,1,1>
$ENDIF
            FOR F = 1 TO MAX
               IF FNAMES<F>="" THEN FPOS=F; EXIT
            NEXT F
            TODAY = EREPLACE(OCONV(DATE(), "D-YMD[4,2,2]"), "-", "")
            TYME  = EREPLACE(OCONV(TIME(), "MTS"), ":", "")
            NOW = TODAY:TYME
            IF FPOS <  MAX THEN
               FNAMES<FPOS>    = zFILE
               FHANDLES(FPOS)  = HANDLE
               LAST.USED<FPOS> = NOW
               LOG.MSG = "SR.FILE.OPEN added ":INFILE:" to commons in position ":FPOS
               IF zFILE # INFILE THEN LOG.MSG := " as ":zFILE
               IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
            END ELSE
               * find the oldest fhandle, close the oldest,        *
               *  set fnames and fhandles to "" then this zFILE    *
               OLDEST = NOW
               OKAY   = 0
               rPOS   = 1
               FOR I = 20 TO MAX
                  IF FNAMES<I> # "" THEN
                     IF LAST.USED<I> < OLDEST THEN
                        OLDEST = LAST.USED<I>
                        rPOS = I
                        OKAY = 1
                     END
                  END
               NEXT I
               IF OKAY THEN
                  LOG.MSG = "SR.FILE.OPEN reached limit of FHANDLES(":MAX:") - CLOSE and swap-out [":FNAMES<rPOS>:"]"
                  CALL SR.FILE.CLOSE(ERR, FNAMES<rPOS>)
                  IF ERR # "" THEN HANDLE=""; RETURN
                  IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
                  GO TRY..OPEN
               END ELSE
                  LOG.MSG = "SR.FILE.OPEN reached limit of FHANDLES(":MAX:") ":INFILE:" opened but not saved."
                  IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
               END
            END
         END ELSE
            IF FAIL.CNT = 0 THEN
               CALL SR.GET.PROPERTY("remaccess", REM.ACCESS)
               IF UPCASE(REM.ACCESS) = "U2SET" THEN REM.ACCESS = 1
               IF REM.ACCESS THEN
                  CALL SR.GET.PROPERTY("u2host", REM.HOST)
                  CALL SR.GET.PROPERTY("u2port", REM.PORT)
                  CALL SR.GET.PROPERTY("u2acct", REM.ACCT)
                  CALL SR.GET.PROPERTY("u2user", REM.USER)
                  CALL SR.GET.PROPERTY("u2pwd" , REM.PASS)
                  CMD = "U2-SET":@FM:REM.HOST:@FM:REM.PORT:@FM:REM.ACCT:@FM:REM.USER:@FM:REM.PASS:@FM
                  EXECUTE CMD CAPTURING JUNK
                  LFILE = INFILE
                  DCT = ""
                  ACC = ""
                  RFILE = INFILE
                  UPD = ""
                  CMD = "U2-VIEW":@FM:LFILE:@FM:DCT:@FM:ACC:@FM:RFILE:@FM:UPD:@FM
                  EXECUTE CMD CAPTURING JUNK
                  FAIL.CNT += 1
                  GO TRY..OPEN
               END ELSE
                  LOG.MSG = "SR.FILE.OPEN could not open [":usefile:"]. Checking ":VFILE:" in ":useacct
                  IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
                  *
                  * Check if usefile is in the VOC of the useacct.
                  *
                  VREC = "Q":@FM:useacct:@FM:VFILE
                  LOCATE(VFILE, FNAMES; vPOS) THEN
                     VID = "qupl_":usefile:"_":useacct
                     WRITE VREC ON FHANDLES(vPOS), VID
                     OPEN VID TO CHK.IO THEN
                        READ CHK FROM CHK.IO, usefile THEN
                           LOG.MSG = "SR.FILE.OPEN found [":usefile:"] in account [":useacct:"]. Pause (2 seconds) and try again."
                           IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
                           FAIL.CNT += 1
                           SLEEP 2000  ; * Wait 2 seconds - may be a Cloud or Network issue.
                           LOG.MSG = "This may have beed caused by DB loads. Trying agin now."
                           IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
                           GO TRY..OPEN
                        END
                        LOG.MSG = "SR.FILE.OPEN created [":VID:"] in [":VFILE:"] - leaving this pointer in [":VFILE:"]"
                        IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
                     END ELSE
                        LOG.MSG = "SR.FILE.OPEN **ERROR** could not create [":VID:"] in [":VFILE:"] - cannot retry to open [":zFILE:"]"
                        IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
                     END
                  END ELSE
                     LOG.MSG = "SR.FILE.OPEN **ERROR** could not pause and retry as [":VFILE:"] has not been opened"
                     IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
                  END
               END
            END
            ERR=INFILE:" FILE NOT FOUND"
            HANDLE = ""
         END
      END
      *
      IF REM.ACCESS THEN
         OPEN "BP.UPL" TO BPIO THEN
         READU REMFILES FROM BPIO, "REMOTE.FILES" ELSE REMFILES = ""
$IFDEF isRT
            LOCATE zFILE IN REMFILES SETTING FPOS ELSE REMFILES<-1> = zFILE
$ELSE
            LOCATE(zFILE, REMFILES; FPOS) ELSE REMFILES<-1> = zFILE
$ENDIF
         END
         WRITE REMFILES ON BPIO, "REMOTE.FILES"
         CLOSE BPIO
         RELEASE
      END
      RETURN
   END
