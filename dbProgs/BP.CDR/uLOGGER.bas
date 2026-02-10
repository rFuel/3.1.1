      SUBROUTINE uLOGGER (LEVEL, INBOUND)
      COMMON /uLOGGER/ uSize, showdt, uMax, jlog
$INCLUDE I_Prologue
      *
      IF NOT(INF.LOGGING) THEN RETURN
      IF UNASSIGNED(uSize) OR uSize = 0 THEN
         uMax=""; uSize=""; showdt=""; jlog=0
         *
         CALL SR.GET.PROPERTY("ulog.max", uMax)
         IF uMax = "" THEN uMax = 10
         *
         CALL SR.GET.PROPERTY("ulog.size", uSize)
         IF uSize = "" THEN uSize = 1048576  ;* 1 megabyte
         *
         CALL SR.GET.PROPERTY("showdt", showdt)
         IF showdt=0 OR UPCASE(showdt)="FALSE" THEN showdt=@FALSE ELSE showdt=@TRUE
         *
         CALL SR.GET.PROPERTY("jlogs", jlog)
         IF jlog=0 OR UPCASE(jlog)="FALSE" THEN jlog=@FALSE ELSE jlog=@TRUE
      END
      *
      IF NOT(ASSIGNED(INBOUND)) THEN INBOUND = " empty message "
      IF DBT="" OR DBT = 0 THEN DBT="UV"            
      IF (DCOUNT(INBOUND, @FM) <= 1) THEN
         KEY = "uplLOG"
         MESSAGE = INBOUND
      END ELSE
         KEY = TRIM(INBOUND<1>)
         MESSAGE = INBOUND<2>
         BEGIN CASE 
            CASE INDEX(UPCASE(KEY), "PHANTOM", 1)
               KEY = "uplLOG"
               MESSAGE = TRIM(EREPLACE(@FM, " ", INBOUND))
            CASE LEN(TRIM(KEY)) < 2
               KEY = MEMORY.VARS(1)
               IF KEY="" THEN 
                  KEY="uplLOG"
                  MEMORY.VARS(1) = KEY
               END
         END CASE
      END
      IF showdt THEN
         TODAY = OCONV(DATE(), "D-YMD[4,2,2]")
         PRECISION 9
         TYME  = TIME()
         MSX   = (FIELD(TYME, ".", 2):"000") "L#5"
         TYME  = OCONV(TYME, "MTS")
         NOW = TODAY:" ":TYME:".":MSX:"  "
      END ELSE
         NOW = ""
         TODAY = ""
         TYME = ""
         MSX = ""
      END
TRY..AGAIN:
      LOGGING=0
      ERR=""
      FND=0
$IFDEF isRT
      USEFILE = "uLOG"
      LOOKIN  = FNAMES
      LOCATE USEFILE IN LOOKIN SETTING FPOS THEN FND=1
$ELSE
      USEFILE = "uLOG,":KEY
      LOOKIN  = FNAMES
      LOCATE(USEFILE, LOOKIN; FPOS) THEN FND=1
$ENDIF
      IF FND THEN
         LOGIO = FHANDLES(FPOS)
$IFDEF isRT
         IF LOGIO = "" THEN NOT.OPEN=1 ELSE NOT.OPEN=0
$ELSE
         NOT.OPEN = (FILEINFO(LOGIO, 0) = 0)
$ENDIF
         IF NOT.OPEN THEN
            FNAMES<FPOS> = ""
            FHANDLES(FPOS) = ""
            GO TRY..AGAIN
         END
      END ELSE
         TRIES=0
         LOOP
$IFDEF isRT
            OPEN "uLOG" TO LOGIO THEN fOPEN=1 ELSE fOPEN=0
$ELSE
            OPENSEQ "uLOG",KEY TO LOGIO THEN fOPEN=1 ELSE fOPEN=0
$ENDIF
            IF fOPEN THEN
               EOFL = DCOUNT(FNAMES, @FM) + 2
               FPOS = EOFL
               FOR F = 1 TO EOFL
                  IF FNAMES<F>="" THEN FPOS=F; EXIT
               NEXT F
               FNAMES<FPOS> = USEFILE
               FHANDLES(FPOS) = LOGIO
               EXIT
            END ELSE
               IF TRIES > 2 THEN
                  PRINT "uLOG may have write-permission issues"
                  STOP
               END
               uLOG="" ; CALL SR.OPEN.CREATE(ERR, "uLOG", "1", COMO)
               IF NOT(ERR) THEN
                  WRITE '' ON COMO, KEY
                  CLOSE COMO
$IFDEF isRT
                  OPEN "uLOG" TO LOGIO ELSE
                     PRINT "uLOG or MD may have write-permission issues"
                     STOP
                  END
$ELSE
                  OPENSEQ "uLOG",KEY TO LOGIO THEN
                     WEOFSEQ LOGIO
                     FLUSH LOGIO ELSE NULL
                     CLOSESEQ LOGIO
                  END ELSE
                     PRINT "uLOG or VOC may have write-permission issues"
                     STOP
                  END
$ENDIF
               END ELSE
                  PRINT "uLOG is missing"
                  STOP
               END
               TRIES += 1
            END
         REPEAT
      END
      
      IF LEVEL = 99 THEN
$IFDEF isRT
         READ REC FROM LOGIO, KEY ELSE REC = ""
         SYZE = LEN(REC)
$ELSE
         STATUS lSTATS FROM LOGIO ELSE lSTATS=""
         SYZE = lSTATS<6>+512
$ENDIF
         IF SYZE > uSize THEN 
            GOSUB FLIP..LOG
            GO TRY..AGAIN
         END
      END
      
      IF NOT(ERR) THEN LOGGING=1 ELSE MESSAGE = ERR
      
      IF LOGGING THEN
$IFDEF isRT
         READ REC FROM LOGIO, KEY ELSE REC = ""
         SYZE = LEN(REC)
$ELSE
         STATUS lSTATS FROM LOGIO ELSE lSTATS=""
         SYZE = lSTATS<6>+512
$ENDIF
         IF SYZE > uSize THEN 
            GOSUB FLIP..LOG
            GO TRY..AGAIN
         END
         * -------------------------------------------------
         IF (jlog) THEN
            IF CORRID = "" THEN CORRID = "uHARNESS"
            LLN = '{"CorrelationID":"':CORRID:'",'
            LLN:= '"Date":"':TODAY:'",'
            LLN:= '"Time":"':TYME:'.':MSX:'",'
            LLN:= '"Caller":"':KEY:'",'
            LLN:= '"Event":"':MESSAGE:'"}'
            MESSAGE = LLN
            NOW = ""
            LLN = ""
         END
         * -------------------------------------------------
$IFDEF isRT
         REC := @FM:NOW:MESSAGE
         WRITE REC ON LOGIO, KEY
$ELSE
         LOGREC = NOW:MESSAGE
         * Find the EOF for LOGIO                 
         offset = 0
         relto  = 2 ; * EOF
         SEEK LOGIO, offset, relto THEN 
            WRITESEQ LOGREC ON LOGIO ELSE NULL
         END ELSE 
            PRINT "uLOG SEEK to end-of-file has ABORTed."
            STOP
         END
$ENDIF
      END
      RETURN
      * 
FLIP..LOG:
      *
      ERR = ""
      OPEN "uLOG"          TO COMO      ELSE ERR := "Cannot Open uLOG"
      OPEN "DICT","uLOG"   TO D.uLOG    ELSE ERR := "Cannot Open DICT uLOG"
      IF ERR # "" THEN
         PRINT ERR
         STOP
      END
      dKEY = "@LAST.":KEY
      READU LAST.NBR FROM D.uLOG, dKEY ELSE LAST.NBR=0
      LAST.NBR += 1
      IF LAST.NBR > uMax THEN LAST.NBR = 1
      WRITE LAST.NBR ON D.uLOG, dKEY
      RELEASE D.uLOG, dKEY
      CLOSE D.uLOG
      *
$IFDEF isRT
      READ LOGREC FROM COMO, KEY ELSE LOGREC = ""
      IF jlog THEN
         LOGREC = ""
      END ELSE
         LOGREC := @FM:NOW:KEY:" has exceeded max size. Saving to ":KEY:".":LAST.NBR
      END
      WRITE LOGREC ON COMO, KEY:".":LAST.NBR
      WRITE ""     ON COMO, KEY
$ELSE
      SEEK LOGIO, 0 , 2 THEN 
         IF (jlog) THEN
            IF CORRID = "" THEN CORRID = "uHARNESS"
            MESSAGE = KEY:" has exceeded max size. Saving to ":KEY:".":LAST.NBR
            LLN = '{"CorrelationID":"':CORRID:'",'
            LLN:= '"Date":"':TODAY:'",'
            LLN:= '"Time":"':TYME:'.':MSX:'",'
            LLN:= '"Caller":"':KEY:'",'
            LLN:= '"Event":"':MESSAGE:'"}'
            LOGEOF = LLN
            NOW = ""
            LLN = ""
         END ELSE
            LOGEOF = NOW:KEY:" has exceeded max size. Saving to ":KEY:".":LAST.NBR
         END
         WRITESEQ LOGEOF ON LOGIO ELSE NULL
       END ELSE 
         PRINT "uLOG SEEK to end-of-file has ABORTed."
         STOP
      END
      *
      * CLOSE the old uHARNESS log
      *
      LOCATE USEFILE IN FNAMES<1> SETTING FPOS ELSE 
         EOFL = DCOUNT(FNAMES, @FM) + 2
         FPOS = EOFL
         FOR F = 1 TO EOFL
            IF FNAMES<F>="" THEN FPOS=F; EXIT
         NEXT F
      END
      *
      WEOFSEQ LOGIO ON ERROR  PRINT "WEOFSEQ error"
      FLUSH LOGIO ELSE        PRINT "FLUSH error"
      CLOSESEQ LOGIO ON ERROR PRINT "CLOSESEQ error"
      CLOSESEQ FHANDLES(FPOS) ON ERROR PRINT "CLOSESEQ FHANDLES error"
      
      LOGIO = ""
      *
      * Read it and write it to the @NEXT log id
      *
      READ  OLDLOG FROM COMO, KEY ELSE OLDLOG = ""
      nKEY = KEY:".":LAST.NBR
      WRITE OLDLOG   ON COMO, nKEY ELSE NULL
      DELETE COMO, KEY
      FNAMES<FPOS>   = ""
      FHANDLES(FPOS) = ""
$ENDIF
      *
      * CLEAR the uHARNESS log and prepare to restart logging
      *
      LOGMSG = NOW:KEY:" Reset /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\"
      OKAY = 1
$IFDEF isRT
      WRITE LOGMSG ON COMO, KEY ON ERROR OKAY = 0
$ELSE
      WRITE LOGMSG ON COMO, KEY ELSE OKAY = 0
$ENDIF
      IF NOT(OKAY) THEN
         PRINT "WRITE FAILURE on uLOG ":KEY:"  Status code [":STATUS():"]"
         STOP
      END
      * 
      RETURN
   END
