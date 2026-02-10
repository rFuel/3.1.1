$INCLUDE I_Prologue
      * --------------------------------------------------------------- *
      * This program sends current records TO the delta log as part     *
      * of the INITIAL load process.                                    *
      * BEFORE starting this program as a phantom;                      *
      * a) ensure delta logging is turned on (rCDC or uCDC)             *
      * b) ensure uStreams is receiving delta records in its topic      *
      * c) ensure uStreams has it's own topic, separate from deltas     *
      * --------------------------------------------------------------- *
      PROG = "DATA.TRICKLE"
$IFDEF isRT
      VFILE = "MD"
      CMD = SENTENCE()
$ELSE
      VFILE = "VOC"
      CMD = @SENTENCE
$ENDIF
      OPEN "UPL.CONTROL"   TO CONTROL ELSE STOP "No UPL.CONTROL file"
      OPEN VFILE           TO VOC     ELSE STOP "No ":VFILE:" file"
      OPEN "uDELTA.LOG"    TO DELTAS  ELSE STOP "No uDELTA.LOG file"
      dot = '_' ; SLASH = "/" ; EXTN = ".ulog"
      atIM = "<im>"  ; atTM = "<tm>" ; atKM = "<km>"
      atFM = "<fm>"  ; atVM = "<vm>" ; atSM = "<sm>"
      READ ENC FROM CONTROL, "@ENCRYPT" ELSE ENC = ""
      IF ENC # "" THEN ENC = 1 ELSE ENC = 0
      *
      CONVERT " " TO @FM IN CMD
      iSPHANTOM = 0
      LOOP WHILE CMD<1> # "" DO
         IF UPCASE(CMD<1>) = "PHANTOM" THEN iSPHANTOM = 1
         IF CMD<1> = PROG THEN EXIT
         CMD = DELETE(CMD, 1, 0, 0)
      REPEAT
* ------------------------------------------------------------------------
      oCOMMAND = CMD
      CONVERT @FM TO " " IN oCOMMAND
      LOG.KEY = PROG:@FM
      LOG.PFX = "======== "
      LOG.MSG = oCOMMAND:" ":LOG.PFX
      CALL uLOGGER(1, LOG.KEY:LOG.PFX:LOG.MSG)
      IF CMD = PROG THEN 
         CALL uLOGGER(1, LOG.KEY:"*")
         CALL uLOGGER(1, LOG.KEY:"*")
         CALL uLOGGER(1, LOG.KEY:"*")
         CALL uLOGGER(1, LOG.KEY:STR("*", 100))
         LOG.MSG = "Automatically phantom trickle all registered files."
         CALL uLOGGER(1, LOG.KEY:LOG.PFX:LOG.MSG)
         GOSUB RUN..ALL..FILES
         GO END..PROG
      END ELSE 
         ACCT = CMD<2>
         FILE = CMD<3>
         pCNT = CMD<4>
         sWAIT= CMD<5>
         IF ACCT = "" OR FILE = "" OR pCNT = "" OR sWAIT = "" THEN
            LOG.MSG = "Usage is: DATA.TRICKLE {account} {file} {process} {pause}"
            CALL uLOGGER(1, LOG.KEY:LOG.PFX:LOG.MSG)
            STOP
         END
         LOG.PFX = ACCT:" ":FILE:" >> "
      END
      NAP 5 
      *
* ------------------------------------------------------------------------
      VREC = "Q":@FM:ACCT:@FM:FILE
      QFL  = "upl_":ACCT:"_":FILE
      WRITE VREC ON VOC, QFL
      OPEN QFL TO IOFILE ELSE 
         LOG.MSG = "Cannot access ":QFL
         CALL uLOGGER(1, LOG.KEY:LOG.PFX:LOG.MSG)
         STOP 
      END 
      LOG.MSG = "Processing ":ACCT:" ":FILE:" ":pCNT:" ":sWAIT
      CALL uLOGGER(1, LOG.KEY:LOG.PFX:LOG.MSG)
      SELECT IOFILE
      CNT=0; TCNT=0
      LOOP
         READNEXT ID ELSE EXIT
         CNT+=1 ; TCNT+=1
         IF CNT > pCNT THEN
            CLOSE DELTAS
            RQM
            SLEEP sWAIT
            CNT=0
            LOG.MSG = TCNT:" records taken."
            CALL uLOGGER(1, LOG.KEY:LOG.PFX:LOG.MSG)
            OPEN 'uDELTA.LOG' TO DELTAS ELSE
               LOG.MSG = "No uDELTA.LOG file"
               CALL uLOGGER(1, LOG.KEY:LOG.PFX:LOG.MSG)
               STOP 
            END 
         END
         ITEM = ID
         READ RECORD FROM IOFILE, ID ELSE CONTINUE
         GOSUB EVENT..LOG
      REPEAT
      DELETE VOC, QFL
      LOG.MSG = TCNT:" records taken."
      CALL uLOGGER(1, LOG.KEY:LOG.PFX:LOG.MSG)
END..PROG:
      LOG.MSG = "Done."
      CALL uLOGGER(1, LOG.KEY:LOG.PFX:LOG.MSG)
      STOP
      *
      * ========================================================
      *
EVENT..LOG:
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
         IF LCK.CNT > 999 THEN
            LOG.MSG = "More than 999 occurances of ":ITEM:" in ":FILE:"  from ":ACCT
            CALL uLOGGER(1, LOG.KEY:LOG.PFX:LOG.MSG)
            SLEEP 3
            LCK.CNT = 1
         END
         KEY = eDate:eTime:LCNT:EXTN
         HDR = ACCT:atIM:FILE:atIM:ITEM:atIM
         READU MT.REC FROM DELTAS, KEY ELSE 
            NREC = RECORD
            NREC = EREPLACE(NREC, @SM, atSM)
            NREC = EREPLACE(NREC, @VM, atVM)
            NREC = EREPLACE(NREC, @FM, atFM)
            NREC = HDR:NREC
            WRITE NREC ON DELTAS, KEY
            RELEASE DELTAS, KEY
            EXIT
         END
         RELEASE DELTAS, KEY
         LCK.CNT+=1
      REPEAT
      RETURN
      *
      * ========================================================
      *
RUN..ALL..FILES:
      READ REGISTER FROM CONTROL, "CDC.REGISTER" ELSE
         LOG.MSG = "FATAL: UPL.CONTROL file must have a valid CDC.REGISTER item."
         CALL uLOGGER(1, LOG.KEY:LOG.PFX:LOG.MSG)
         RETURN
      END
      LOG.PFX = ">> RUN-ALL "
      READ DEFAULTS FROM CONTROL, "CDC.DEFAULTS" ELSE DEFAULTS = ""
      PCNT = 0 ; sWAIT = 0
      AAAA = 0 ; BBBB = 0
      CALL SR.KEYVALUE(DEFAULTS, "pcnt", AAAA)
      CALL SR.KEYVALUE(DEFAULTS, "wait", BBBB)
      IF NOT(NUM(AAAA))  THEN AAAA = 1000
      IF AAAA+0 = 0      THEN AAAA = 1000
      IF NOT(NUM(BBBB))  THEN BBBB= 5
      IF BBBB+0= 0       THEN BBBB= 5
      PCNT = AAAA
      sWAIT= BBBB
      EOI = DCOUNT(REGISTER, @FM)
      FOR I = 1 TO EOI 
         LINE = EREPLACE(REGISTER<I>, "  ", " ")
         ACCT = FIELD(LINE, " ", 1)
         FILE = FIELD(LINE, " ", 2)
         CMD  = "PHANTOM DATA.TRICKLE ":ACCT:" ":FILE:" ":PCNT:" ":sWAIT
         LOG.MSG = CMD
         CALL uLOGGER(1, LOG.KEY:LOG.PFX:LOG.MSG)
         EXECUTE CMD CAPTURING JUNK
      NEXT I
      RETURN
      *
   END
