      SUBROUTINE SR.FETCH.PREP (MAT IN.STRINGS)
$INCLUDE I_Prologue
      * --------------------------------------------------------
      * Equate local variables to call string locations         
      * Subroutine API:                                         
      * --------------------------------------------------------
$IFDEF isRT
      DIM IN.STRINGS(20)
$ENDIF
      EQU ANS           TO IN.STRINGS(1)
      EQU SEL           TO IN.STRINGS(2)
      EQU RUNTYPE       TO IN.STRINGS(3)
      EQU FILE          TO IN.STRINGS(4)
      EQU CORRELATION   TO IN.STRINGS(5)
      EQU aSTEP         TO IN.STRINGS(6)
      EQU DACCT         TO IN.STRINGS(7)
      * --------------------------------------------------------
      CALL uLOGGER(99, LOG.KEY:"Roll log over")
      LOG.KEY = "uplLOG":@FM
      LOG.MSG = "SR.FETCH.PREP Started"
      CALL uLOGGER(0, LOG.KEY:LOG.MSG)
      * --------------------------------------------------------
      PREPARED.LIST = ""
      * --------------------------------------------------------
      IF SEL # "" THEN
         PREPARED.LIST = CORRELATION:"_fPREP"
         CMD = "DELETE.LIST ":PREPARED.LIST
         CALL uLOGGER(0, "Executing: ":CMD)
         EXECUTE CMD CAPTURING uvReply
         CALL uLOGGER(0, "         : ":uvReply)
         * -----------------------------------------------------
         CMD = FIELD(SEL, "<tm>", 1)   ;*  SELECT command
         CALL uLOGGER(0, "Executing: ":CMD)
         EXECUTE CMD RTNLIST SEL.LIST0 CAPTURING uvReply
         CALL uLOGGER(0, "         : ":uvReply)
         * -----------------------------------------------------
         CMD = FIELD(SEL, "<tm>", 2)   ;* NSELECT command
         IF CMD # "" THEN
            CALL uLOGGER(0, "Executing: ":CMD)
            EXECUTE CMD PASSLIST SEL.LIST0 RTNLIST SEL.LIST1 CAPTURING uvReply
            CALL uLOGGER(0, "         : ":uvReply)
            CMD = "SAVE.LIST ":PREPARED.LIST
            EXECUTE CMD PASSLIST SEL.LIST1 CAPTURING uvReply
         END ELSE
            CMD = "SAVE.LIST ":PREPARED.LIST
            CALL uLOGGER(0, "Executing: ":CMD)
            EXECUTE CMD PASSLIST SEL.LIST0 CAPTURING uvReply
            CALL uLOGGER(0, "         : ":uvReply)
         END
      END
      * --------------------------------------------------------
      IF RUNTYPE = "" THEN RUNTYPE = "FULL"
      ANS = ""
      BP.UPL = "" ; CALL SR.FILE.OPEN(ERR, "BP.UPL", BP.UPL)
      IF NOT(ERR) THEN
         READ DBT FROM BP.UPL, "DBT" ELSE 
            DBT="UV"
            CALL uLOGGER(0, "BP.UPL does not have DBT! Default to UV")
            WRITE DBT ON BP.UPL, "DBT"
         END
         BP.UPL = ""
      END ELSE
         CALL uLOGGER(0, "BP.UPL cannot be accessed!")
         STOP
      END
      CALL uLOGGER(0, "SR.FETCH.PREP started for ":FILE)
      VOC = "" ; CALL SR.FILE.OPEN(ERR, "VOC", VOC)
      IF ERR THEN CALL uLOGGER(0, "No VOC file"); STOP
      TRY=0
      LOOP
         TRY+=1
         IF TRY > 2 THEN
            CALL uLOGGER(0, " <<FATAL>> Cannot open or setup ":FILE)
            JUNK=""; GO RETN
         END
         READ REC FROM VOC, FILE THEN
            EXIT
         END ELSE
            IF DBT="UV" THEN REC = "Q":@FM:DACCT:@FM:FILE
            IF DBT="UD" THEN REC = "F":@FM:DACCT:FILE:@FM:DACCT:"D_":FILE
            WRITE REC ON VOC, FILE
            WRITE REC ON VOC, FILE:".QF"
            CALL uLOGGER(0, " Q-pointers created for ":FILE)
         END
      REPEAT
      * --------------------------------------------------------
      EXTN = ".TAKE"    ; GOSUB CHECK..FILE
      EXTN = ".LOADED"  ; GOSUB CHECK..FILE
      * --------------------------------------------------------
      CALL uLOGGER(0, " Clearing old select-lists")
      IF DBT="UV" THEN SL = "&SAVEDLISTS&"
      IF DBT="UD" THEN SL = "SAVEDLISTS"
      IF INDEX(CORRELATION, FILE, 1) THEN
         EXE = "SELECT ":SL:" WITH @ID LIKE ":CORRELATION:"_..." 
      END ELSE
         EXE = "SELECT ":SL:" WITH @ID LIKE ":CORRELATION:FILE:"_..." 
      END
      CALL uLOGGER(0, EXE)
      EXECUTE EXE CAPTURING JUNK
      DEL = "DELETE ":SL:" "
      LOOP
         READNEXT ID ELSE EXIT
         EXECUTE DEL:ID CAPTURING JUNK
      REPEAT
      * --------------------------------------------------------
      CALL uLOGGER(0, " >>        file=":FILE)
      CALL uLOGGER(0, " >> correlation=":CORRELATION)
      CALL uLOGGER(0, " >>       aStep=":aSTEP)
      CALL uLOGGER(0, " >>     runtype=":RUNTYPE)
      CALL uLOGGER(0, " >>    pre-list=":PREPARED.LIST)
      CMD = "PHANTOM uPREP ":FILE:" ":CORRELATION:" ":aSTEP:" ":RUNTYPE:" ":PREPARED.LIST
      CALL uLOGGER(0, " >> ":CMD)
      EXECUTE CMD CAPTURING JUNK
      CALL uLOGGER(0, " >> ":JUNK)
      CALL uLOGGER(0, "SR.FETCH.PREP finished for ":FILE)
RETN:
      *CLOSE
      ANS = "ok"
      RQM ; RQM ; SLEEP 3
      RETURN
**************************************************************************************
*
CHECK..FILE:
      * watch for INCR where FILE = xyz.TAKE       
      IF FILE[LEN(FILE)-4,5]=".TAKE" THEN RETURN
      * -------------------------------------------
      CALL SR.OPEN.CREATE(ERR, FILE:EXTN, "DYNAMIC", JUNKIO)
      IF ERR THEN
         CALL uLOGGER(0, "****************************************")
         CALL uLOGGER(0, FILE:EXTN:" cannot be created!")
         CALL uLOGGER(0, JUNK)
         CALL uLOGGER(0, "****************************************")
         STOP
      END
      CLOSE JUNKIO
      RETURN
   END

