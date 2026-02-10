$INCLUDE I_Prologue
      EXTN = ".ulog"
      atIM = "<im>"  ; atTM = "<tm>" ; atKM = "<km>"
      atFM = "<fm>"  ; atVM = "<vm>" ; atSM = "<sm>"
$IFDEF isRT
      VFILE = "MD"
      CMD = SENTENCE()
$ELSE
      VFILE = "VOC"
      CMD = @SENTENCE
$ENDIF
      OPEN VFILE              TO VOC            ELSE STOP "No ":VFILE:" file"
      OPEN "BP.UPL"           TO BP.UPL         ELSE STOP "No BP.UPL file"
      OPEN "uDELTA.LOG"       TO DELTAS         ELSE STOP "No uDELTA.LOG file"
      OPEN "STREAM.CONTROL"   TO STREAM.CONTROL ELSE STOP "No STREAM.CONTROL file"
      READ ENC FROM BP.UPL, "@ENCRYPT" ELSE ENC = ""
      IF ENC # "" THEN ENC = 1 ELSE ENC = 0
      READ DEFAULT FROM STREAM.CONTROL, "@DEFAULT"  ELSE DEFAULT  = 500:@FM:2
      READ CONTROL FROM STREAM.CONTROL, "@RUN"      ELSE CONTROL  = ""
      *
      EOA = DCOUNT(CONTROL, @FM)
      *
      CRT @(-1):
      CRT "UPLIFT.MANAGER             Manage the Uplift of Business Data"
      CRT "==============             ----------------------------------"
      CRT 
      CRT "STREAM.CONTROL @RUN has ":EOA:" account(s) to be loaded"
      CRT 
      FOR A = 1 TO EOA
         ACCT = CONTROL<A>
         READ FILES FROM STREAM.CONTROL, ACCT ELSE FILES = ""
         EOF  = DCOUNT(FILES, @FM)
         CRT "   ) ":ACCT:" has ":EOF:" files to process"
         FOR F = 1 TO EOF
            FILE = FILES<F, 1, 1>
            QPTR = "Q":@FM:ACCT:@FM:FILE
            QFIL = "upl_":ACCT:"_":FILE
            WRITE QPTR ON VOC, QFIL
            OPEN QFIL TO SOURCE.DATA ELSE
               CRT "     > ERROR: cannot access ":QFIL:" skipping it."
               CONTINUE
            END
            RCNT = 0 ; PCNT = FILES<1,2,1>+0 ; PAUZ = FILES<1,3,1>+0
            IF PCNT=0 THEN PCNT = DEFAULT<1>
            IF PAUZ=0 THEN PAUZ = DEFAULT<2>
            CRT "     .) ":FILE
            GOSUB PROCESS..THIS..FILE
            FILES<F> = "--":FILE:@VM:PCNT:@VM:PAUZ
         NEXT F
         WRITE FILES ON STREAM.CONTROL, ACCT
         CONTROL<A> = "--":ACCT:" ":EOF:" files have been processed"
         WRITE CONTROL ON STREAM.CONTROL, "@RUN"
      NEXT A
      CRT
      CRT "Done."
      STOP
      * --------------------------------------------------------------------------
PROCESS..THIS..FILE:
      *
      SELECT SOURCE.DATA
      RCNT = 0
      LOOP
         RCNT+=1
         IF RCNT > PCNT THEN
            CRT "        >>  Closing uDELTA.LOG" 
            CLOSE DELTAS
            FOR PP = 1 TO PAUZ
               RQM
            NEXT PP
            RCNT=0
            OPEN 'uDELTA.LOG' TO DELTAS ELSE STOP "No uDELTA.LOG file"
            CRT "        >>  Opened  uDELTA.LOG" 
         END
         READNEXT ID ELSE EXIT
         READ RECORD FROM SOURCE.DATA, ID ELSE CONTINUE
         ITEM = ID
         GOSUB LOG..EVENT
      REPEAT
      RETURN
      * --------------------------------------------------------------------------
LOG..EVENT:
      eDate = OCONV(DATE(), "D4-DMY[2,2,4]")
      YY    = FIELD(eDate, "-", 3)
      MM    = FIELD(eDate, "-", 2)
      DD    = FIELD(eDate, "-", 1)
      eDate = YY:MM:DD
      eTime = OCONV(TIME(), "MTS")
      eTime = EREPLACE(eTime, ":", "")
      LCK.CNT=0
      LOOP
         LCNT = ("000":LCK.CNT) "R#3"
         IF LCNT > 999 THEN
            CRT 
            CRT "More that 999 occurances of ":ITEM:" in ":FILE:" from ":ACCT:" being locked."
            CRT 
            RETURN
         END
         KEY = eDate:eTime:LCNT:EXTN
         HDR = ACCT:atIM:FILE:atIM:ITEM:atIM
         READU MT.REC FROM DELTAS, KEY ELSE 
            NREC = RECORD
            NREC = EREPLACE(NREC, @SM, atSM)
            NREC = EREPLACE(NREC, @VM, atVM)
            NREC = EREPLACE(NREC, @FM, atFM)
            IF ENC THEN
               EREC = ""
               CALL SR.ENCRYPT(EREC, NREC)
               NREC = EREC
            END
            NREC = HDR:NREC
            WRITE NREC ON DELTAS, KEY
            RELEASE DELTAS, KEY
            EXIT
         END
         RELEASE DELTAS, KEY
         LCK.CNT+=1
      REPEAT
      RETURN
