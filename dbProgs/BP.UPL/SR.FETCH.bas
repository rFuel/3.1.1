******SUBROUTINE SR.FETCH(ANS, FILE, aFROM, aTO, rQM, CORRELATION)
      SUBROUTINE SR.FETCH (MAT IN.STRINGS)
$INCLUDE I_Prologue
      * --------------------------------------------------------
      * Equate local variables to call string locations         
      * Subroutine API:
      ********** this is depricated. See uFETCH.bas ************
      * --------------------------------------------------------
$IFDEF isRT
      DIM IN.STRINGS(20)
$ENDIF
      EQU ANS           TO IN.STRINGS(1)
      EQU FILE          TO IN.STRINGS(2)
      EQU aFROM         TO IN.STRINGS(3)
      EQU aTO           TO IN.STRINGS(4)
      EQU rQM           TO IN.STRINGS(5)
      EQU CORRELATION   TO IN.STRINGS(6)
      * --------------------------------------------------------
      LOG.KEY = "uplLOG":@FM
      CALL uLOGGER(0, LOG.KEY:" << SR.FETCH *************************************************")
      CALL uLOGGER(0, LOG.KEY:" << SR.FETCH ":FILE:"  From [":aFROM:"]  to  [":aTO:"]")
      *
      ANS = ""; DBT="ERR"; CHK.ENCRYTED=""
      SKEY=""
      BP.UPL = "" ; CALL SR.FILE.OPEN(ERR, "BP.UPL", BP.UPL)
      IF NOT(ERR) THEN
         READ STOP.SW FROM BP.UPL, "STOP" ELSE STOP.SW=""
         IF STOP.SW="stop" THEN GO RETN
         READ DBT FROM BP.UPL, "DBT" ELSE 
            DBT="UV"
            CALL uLOGGER(0, LOG.KEY:"BP.UPL does not have DBT! Default to UV")
            WRITE DBT ON BP.UPL, "DBT"
         END
         READ CHK.ENCRYTED FROM BP.UPL, "ENC" ELSE CHK.ENCRYTED=""
      END ELSE
         CALL uLOGGER(0, LOG.KEY:"BP.UPL cannot be accessed!")
         STOP
      END
      IF LEN(CHK.ENCRYTED) THEN ENC=1 ELSE ENC=0
      IF (aFROM+0) # aFROM THEN GO RETN
      IF (aTO+0) # aTO THEN GO RETN
      IF aFROM < 1 THEN aFROM = 1
 *
      BASKEY = ""
      IF INDEX(CORRELATION, FILE, 1) THEN
         BASKEY = CORRELATION:"_":aFROM
      END ELSE
         BASKEY = CORRELATION:FILE:"_":aFROM
      END
      SKEY=BASKEY
      ID.ONLY = 0
      IF SKEY[1,8] = "FetchKey" THEN ID.ONLY = 1
      IF SKEY[1,8] = "DeltaKey" THEN ID.ONLY = 1
 *
      IF DBT="UV" THEN SL = "&SAVEDLISTS&"
      IF DBT="UD" THEN SL = "SAVEDLISTS"
      SAVELIST = "" ; CALL SR.FILE.OPEN(ERR, SL, SAVELIST)
      IF ERR THEN
         CALL uLOGGER(0, LOG.KEY:"*** SR.FETCH: cannot access ":SL)
         GO RETN
      END
      IPCT = rQM+0
      IF IPCT = 0 THEN IPCT = 5000
      IOFILE = "" ; CALL SR.FILE.OPEN(ERR, FILE, IOFILE)
      IF ERR THEN
         CALL uLOGGER(0, LOG.KEY:"*** SR.FETCH: cannot access ":FILE)
         GO RETN
      END
      LOOP.CNT=0
      LOOP
         LOOP.CNT += 1
         CALL uLOGGER(0, LOG.KEY:" << SR.FETCH    looking  for ":SKEY:" >> ":LOOP.CNT)
         READ LIST FROM SAVELIST, SKEY THEN 
            IF LIST#"" THEN EXIT
         END
         IF LOOP.CNT > 20 THEN
            CALL uLOGGER(0, LOG.KEY:" @@ SR.FETCH    giving up on ":SKEY)
            GO RETN
         END ELSE
            READ STOP.SW FROM BP.UPL, "STOP" ELSE STOP.SW=""
            IF STOP.SW="stop" THEN
               CALL uLOGGER(0, SKEY:" Stopped - STOP switch set ON!":@FM:STR('*',70))
               GO RETN
            END
            RQM ; RQM ; SLEEP 3
         END
      REPEAT
      EOI = DCOUNT(LIST, @FM)
      CALL uLOGGER(0, LOG.KEY:" << SR.FETCH    End-Of-List at ":EOI)
      FFROM = 1 ; TTO = EOI
      GOOD=""        ;* should be in common & set up in CLEAR.COMMON
      IF NOT(ID.ONLY) AND ENC THEN
         FOR SS = 32 TO 126
            GOOD := CHAR(SS)
         NEXT SS
      END
      *
START..EXTRACT:
      EOL.MK = ""
      ANS = ""
      PCNT = 0
      CRLF = CHAR(13):CHAR(10)
      LOST = 0
      STOP.FLAG=99
      CALL uLOGGER(0, LOG.KEY:" << SR.FETCH    processing   ":SKEY)
      DIM CALL.STRINGS(20)
      LOOP
         IF STOP.FLAG = 0 THEN EXIT
         PCNT += 1
         IPC = PCNT / IPCT
         IF (INT(IPC) = IPC) THEN RQM
         REMOVE ID FROM LIST SETTING STOP.FLAG
         ID = CONVERT(CHAR(013), "", ID)
         IF ID = "[<END>]" THEN
            CALL uLOGGER(0, LOG.KEY:" << SR.FETCH    got [<END>]  ":SKEY)
            ANS := EOL.MK:ID
            EXIT
         END
         IF NOT(ID.ONLY) THEN
            READ TREC FROM IOFILE, ID ELSE 
               CALL uLOGGER(0, LOG.KEY:"    SR.FETCH cannot find ID [":ID:"] in ":FILE)
               LOST+=1; CONTINUE
            END
            IF ENC THEN
               EOA = DCOUNT(TREC, @FM)
               FOR A = 1 TO EOA
                  TMPA = TREC<A>
                  EOM = DCOUNT(TMPA, @VM)
                  FOR M = 1 TO EOM
                     TMPM = TMPA<1,M>
                     EOS = DCOUNT(TMPM, @SM)
                     FOR S = 1 TO EOS
                        TMPS = TMPM<1,1,S>
                        CONVERT GOOD TO '' IN TMPS
                        IF LEN(TMPS) THEN TREC<A,M,S> = "ENCRYTED"
                     NEXT S
                  NEXT M
               NEXT A
            END
            
            MAT CALL.STRINGS = ""
            CALL.STRINGS(2)  = TREC
            CALL SR.ZIP.RECORD (MAT CALL.STRINGS)
            
            LINE = ID:"<im>":CALL.STRINGS(3)
            ANS := EOL.MK:LINE
         END ELSE
            ANS := EOL.MK:ID
         END
         EOL.MK = CRLF
      REPEAT
      IF LOST > 0 THEN 
         CALL uLOGGER(0, LOG.KEY:" @@ SR.FETCH ----------------")
         CALL uLOGGER(0, LOG.KEY:" @@ SR.FETCH Lost ":LOST:" records")
         CALL uLOGGER(0, LOG.KEY:" @@ SR.FETCH ----------------")
      END
RETN:
      RELEASE
      CALL uLOGGER(0, LOG.KEY:" << SR.FETCH    completed    ":SKEY)
      CALL uLOGGER(99, LOG.KEY:"Roll log over")
$IFNDEF isRT
      CLOSE
$ENDIF
      RETURN
   END
