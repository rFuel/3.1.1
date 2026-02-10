      PROMPT ""
      *
      * --------------------------------------------------------
      *  Initialise
      * --------------------------------------------------------
      *
      GOOD=""
      FOR SS = 32 TO 126
         GOOD := CHAR(SS)
      NEXT SS
      CRT "Starting DATA.LOADER -----------------------------------------------------"
      CRT "This must be run as uvadm !"
      STOPPER = "STOP"
      atIM = "<im>"
      atFM = "<fm>"
      atVM = "<vm>"
      atSM = "<sv>"
      DOT  = "_"
      UPL  = "upl_"
      CRLF = CHAR(10)
      OPEN "VOC" TO VOC ELSE 
         CRT "Cannot open VOC"
         STOP
      END
      OPEN "uDELTA.LOG" TO uDELTA ELSE
         CRT "Cannot open uDELTA.LOG"
         STOP
      END
      OPEN "DICT", "uDELTA.LOG" TO CONTROLS ELSE
         CRT "Cannot open DICT uDELTA.LOG"
         STOP
      END
      READ EVT.REGISTER FROM CONTROLS, "@EVT.REGISTER" ELSE
         CRT "Please create @EVT.REGISTER in DICT uDELTA.LOG"
         STOP
      END
***   uvHome  = "/usr/uv/"
      READ uvHome FROM CONTROLS, "@UVHOME" ELSE
         CRT "Please create @UVHOME in DICT uDELTA.LOG"
         STOP
      END
      READ BATCHSIZE FROM CONTROLS, "@BATCH.SIZE" ELSE BATCHSIZE = 1000
      *
      WRITE STOPPER ON CONTROLS, "STOP"
      SLEEP 5
      *
      CRT "Setting pointers to UV AUDLOGs"
      * ------------------------------------------------
      * ALL Audlog files are resident in UV account
      * ------------------------------------------------
      QREC = "Q":@FM:"UV":@FM:"VOC"
      WRITE QREC ON VOC, "uQUV"
      EXE  = "SSELECT uQUV LIKE '&AUDLOG...'"
      EXECUTE EXE CAPTURING JUNK
      LOGMAX = @SELECTED
      IF LOGMAX = 0 THEN
         CRT "No audlog files set up in the UV account!"
         STOP
      END
      DIM LOGNAM(LOGMAX) ; MAT LOGNAM = ""
      DIM AUDLOG(LOGMAX) ; MAT AUDLOG = ""
      I = 0
      LOOP
         READNEXT ID ELSE EXIT
         I += 1
         READ CHK FROM VOC, ID ELSE
            QREC<3> = ID
            WRITE QREC ON VOC, ID
         END
         LOGNAM(I) = ID
         OPEN LOGNAM(I) TO AUDLOG(I) ELSE
            CRT "Failed to open ":LOGNAM(I)
            STOP
         END
      REPEAT
      SUSPEND='sh -c"':uvHome:'bin/audman -suspendlog @"'
      CLEANUP='sh -c"':uvHome:'bin/audman -clearlog @"'
      RESUME ='sh -c"':uvHome:'bin/audman -resumelog @"'
      *
      * --------------------------------------------------------
      *  Process until CONTROLS STOP = stop
      * --------------------------------------------------------
      *
      FNAMES = ""       ; BADFILES = ""
      DIM FHANDLES(200) ; MAT FHANDLES = ""
      OUTREC = ""
      OUTCNT = 0
      UPDCNT = 0
      OUTBLK = BATCHSIZE
      EXTN   = ".ulog"
      TEMP   = ".temp"
      *
      WRITE "" ON CONTROLS, "STOP"
      CRT "Processing ":LOGMAX:" audlogs"
      *
      LOOP
         READ STOP.SW FROM CONTROLS, "STOP" ELSE STOP.SW=""
         IF UPCASE(STOP.SW) = STOPPER THEN
            IF OUTREC # "" THEN GOSUB WRITE..TO..DELTAS
            CRT "Stop switch is on - stopping now"
            STOP
         END
         FOR FNUM = 1 TO LOGMAX
            * --------------------------------------------------
            * Stop UV logging to this file
            EXE = EREPLACE(SUSPEND, "@", FNUM)
            CRT "   .) ":EXE
            EXECUTE EXE CAPTURING OUTPUT
            IF INDEX(OUTPUT, "uvadm", 1) THEN
               CRT OUTPUT<1>:"  !!"
               IF OUTREC # "" THEN GOSUB WRITE..TO..DELTAS
               STOP
            END
            * --------------------------------------------------
            * Scrape deltas from this file
            EXE = "SELECT ":LOGNAM(FNUM)
            CRT "   .) ":EXE
            EXECUTE EXE CAPTURING JUNK
            TODO = @SELECTED
            CRT "   .) ":TODO:" records to process."
            LOOP
               READNEXT ID ELSE EXIT
               READ LOGREC FROM AUDLOG(FNUM), ID ELSE CONTINUE
               IF LOGREC<1> # "DAT.BASIC.WRITE" THEN CONTINUE
               ACT = LOGREC<4>
               FIL = LOGREC<7>
               IID = LOGREC<8>
               IF INDEX(ACT, "/", 1) THEN ACT = FIELD(ACT, "/", DCOUNT(ACT, "/"))
               IF INDEX(FIL, "/", 1) THEN FIL = FIELD(FIL, "/", DCOUNT(FIL, "/"))
               IF ACT = "" THEN CONTINUE
               IF FIL = "" THEN CONTINUE
               IF IID = "" THEN CONTINUE
               EVTLOC = ACT:" ":FIL
               LOCATE(EVTLOC, EVT.REGISTER; FND) ELSE
                  EVTLOC = ACT:" *"
                  LOCATE(EVTLOC, EVT.REGISTER; FND) ELSE
                     CONTINUE
                  END
               END
               QFL = UPL:ACT:"_":FIL
               *
               LOCATE(QFL, BADFILES; FND) THEN CONTINUE
               LOCATE(QFL, FNAMES; FND) ELSE
                  WRITE "Q":@FM:ACT:@FM:FIL ON VOC, QFL
                  OPEN QFL TO JUNKIO THEN
                     FND = DCOUNT(FNAMES, @FM) + 1
                     FNAMES<FND> = QFL
                     FHANDLES(FND) = JUNKIO
                     JUNKIO = ""
                  END ELSE
                     BADFILES<-1> = QFL
                     CRT "Skipping bad file ":QFL
                     CONTINUE
                  END
               END
               HANDLE = FHANDLES(FND)
               *
               READ UVREC FROM HANDLE, IID ELSE CONTINUE
               UVREC = EREPLACE(UVREC, @FM , atFM)
               UVREC = EREPLACE(UVREC, @VM , atVM)
               UVREC = EREPLACE(UVREC, @SM , atSM)
               UVREC = EREPLACE(UVREC, '\', "\\")
               UVREC = EREPLACE(UVREC, '"', '\"')
               EDATE = EREPLACE(OCONV(FIELD(ID, ".", 1), "D4-"), "-", "")
               ETIME = EREPLACE(OCONV(FIELD(ID, ".", 2), "MTS"), ":", "")
               EVENT = '{"passport": "", "sourceinstance": "", '
               EVENT:= '"sourceaccount": "':ACT:'", "date": "':EDATE:'", '
               EVENT:= '"time": "':ETIME:'", "file": "':FIL:'", '
               EVENT:= '"item": "':IID:'", "record": "':UVREC:'"}'
               OUTREC := EVENT : CRLF
               OUTCNT += 1
               IF OUTCNT > OUTBLK THEN GOSUB WRITE..TO..DELTAS
            REPEAT
            * --------------------------------------------------
            IF OUTREC # "" THEN GOSUB WRITE..TO..DELTAS
            OUTCNT = 0
            *
            EXE = EREPLACE(CLEANUP, "@", FNUM)
            CRT EXE
            EXECUTE EXE
            * --------------------------------------------------
            EXE = EREPLACE(RESUME, "@", FNUM)
            CRT EXE
            EXECUTE EXE
         NEXT FNUM
         RQM ; RQM ; RQM
      REPEAT
      CRT "Stop switch is now ON."
      STOP
      *
      * --------------------------------------------------------
      *
WRITE..TO..DELTAS:
      IF OUTREC = "" THEN RETURN
      UPDCNT += 1
      KEY = UPDCNT:EXTN
      LOOP
      READ JUNK FROM uDELTA, KEY ELSE EXIT
         UPDCNT += 1
         KEY = UPDCNT:EXTN
      REPEAT
      WRITE OUTREC ON uDELTA, UPDCNT:EXTN
      OUTREC = ""
      RETURN
END