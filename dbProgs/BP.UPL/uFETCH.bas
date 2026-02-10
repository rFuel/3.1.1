      SUBROUTINE uFETCH(ANS, FILE, aFROM, aTO, rQM, CORRELATION)
$INCLUDE I_Prologue
      *
      IF MEMORY.VARS(1) =  "" THEN MEMORY.VARS(1) = "uplLOG"
      LOG.KEY = MEMORY.VARS(1):@FM
      IF DBT#"UV" AND DBT#"UD" THEN
         UPL.LOGGING = 0
         INF.LOGGING = 0
         MAT sockPROPS = ""
         CALL SR.OPEN.CREATE(ERR, "BP.UPL", "19", BP.UPL)
         IF ERR THEN
            DBT = "UV"
         END ELSE
            READ DBT FROM BP.UPL, "DBT" ELSE DBT = "UV"
            READ PARAMS FROM BP.UPL, "properties" ELSE PARAMS = ""
            MATPARSE sockPROPS FROM PARAMS
            pAns = ""; CALL SR.GET.PROPERTY("upl.logging", pAns) ; UPL.LOGGING = pAns
            pAns = ""; CALL SR.GET.PROPERTY("inf.logging", pAns) ; INF.LOGGING = pAns
            IF PARAMS="" THEN
               PARAMS<1> = UPL.LOGGING
               PARAMS<2> = INF.LOGGING
            END
            WRITE DBT ON BP.UPL, "DBT"
            WRITE PARAMS ON BP.UPL, "properties"
         END
      END
      *
      LOG.LEVEL=0
      ANS = ""; CHK.ENCRYTED=""
      SKEY=""
      ENC = 0
      ENC.SRC = 0                           ;* encrypt at source
      OPEN "BP.UPL" TO BP.UPL THEN
         READ STOP.SW FROM BP.UPL, "STOP" ELSE STOP.SW=""
         IF STOP.SW="stop" THEN GO RETN
         * reuse var CHK.ENCRYTED
         READ CHK.ENCRYTED FROM BP.UPL, "ENC" ELSE CHK.ENCRYTED=""
         IF LEN(CHK.ENCRYTED) THEN ENC=1
         READ CHK.ENCRYTED FROM BP.UPL, "ENC.SOURCE" ELSE CHK.ENCRYTED=""
         IF LEN(CHK.ENCRYTED) THEN ENC.SRC=1
      END ELSE
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:"BP.UPL cannot be accessed!")
         STOP
      END
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:" << uFETCH**************************************************")
      LOG.MSG = " << uFETCH    ":CORRELATION:"  From [":aFROM:"]  to  [":aTO:"]"
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:LOG.MSG)
      IF (aFROM+0) # aFROM THEN GO RETN
      IF (aTO+0) # aTO THEN GO RETN
      IF aFROM < 1 THEN aFROM = 1
      *
      BASKEY = ""
      BASKEY = CORRELATION:"_":aFROM
      SKEY=BASKEY
      ID.ONLY = 0
      IF SKEY[1,8] = "FetchKey" THEN ID.ONLY = 1
      IF SKEY[1,8] = "DeltaKey" THEN ID.ONLY = 1
      SL="THROW-ERROR"
      IF DBT="UV" THEN SL = "&SAVEDLISTS&"
      IF DBT="UD" THEN SL = "SAVEDLISTS"
      OPEN SL TO SAVELIST ELSE
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:"*** uFETCH: cannot access ":SL:"  DBT is ":DBT)
         GO RETN
      END
      IPCT = rQM+0
      IF IPCT = 0 THEN IPCT = 5000
      OPEN FILE TO IOFILE ELSE
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:"*** uFETCH: cannot access ":FILE)
         GO RETN
      END
      LOOP.CNT=0
      LOOP
         LOOP.CNT += 1
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:" << uFETCH    looking  for ":SKEY:" >> ":LOOP.CNT)
         READ LIST FROM SAVELIST, SKEY THEN 
            IF LIST#"" THEN EXIT
         END
         IF LOOP.CNT > 20 THEN
            CALL uLOGGER(LOG.LEVEL, LOG.KEY:" @@ uFETCH    giving up on ":SKEY)
            GO RETN
         END ELSE
            READ STOP.SW FROM BP.UPL, "STOP" ELSE STOP.SW=""
            IF STOP.SW="stop" THEN
               CALL uLOGGER(LOG.LEVEL, LOG.KEY:" Stopped - STOP switch set ON!":@FM:STR('*',70))
               GO RETN
            END
            RQM ; RQM ; SLEEP 3
         END
      REPEAT
      EOI = DCOUNT(LIST, @FM)
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:" << uFETCH    End-Of-List at ":EOI)
      FFROM = 1 ; TTO = EOI
      GOOD=""
      IF NOT(ID.ONLY) AND ENC THEN
         FOR SS = 32 TO 126
            GOOD := CHAR(SS)
         NEXT SS
      END
      * Mod - char(13)+char(10) found in data at Kiwibank
      BAD = ""
      FOR C = 0 TO 31
          BAD := CHAR(C)
      NEXT C
      *********** @@ uv2sql ***********
      atIM   = "<im>"   ;*"!"      ;*
      atFM   = "<fm>"   ;*TILDE    ;* CHAR(253) ;*
      atVM   = "<vm>"   ;*"`"      ;* CHAR(252) ;*
      atSV   = "<sm>"   ;*CHAR(094);* CHAR(251) ;*
      *********** @@ uv2sql ***********
START..EXTRACT:
      PAYLOAD.SIZE = 0
      EOL.MK = ""
      ANS = ""
      PCNT = 0
      HIT.END=0
      CRLF = CHAR(13):CHAR(10)
      LOST = 0
      STOP.FLAG=99
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:" << uFETCH    processing   ":SKEY)
      LOOP
         IF STOP.FLAG = 0 THEN EXIT
         PCNT += 1
         IPC = PCNT / IPCT
         IF (INT(IPC) = IPC) THEN RQM
         REMOVE ID FROM LIST SETTING STOP.FLAG
         ID = CONVERT(CHAR(013), "", ID)
         IF ID = "" THEN CONTINUE
         IF ID = "[<END>]" THEN
            IF NOT(HIT.END) THEN
               CALL uLOGGER(LOG.LEVEL, LOG.KEY:" << uFETCH    got [<END>] after ":PCNT:" record(s) loaded")
               ANS := EOL.MK:ID
               HIT.END = 1
               EOL.MK = CRLF
            END
            CONTINUE
         END
         IF ID = "[<EOP>]" THEN
            CALL uLOGGER(LOG.LEVEL, LOG.KEY:" << uFETCH    got [<EOP>] after ":PCNT:" record(s) loaded")
            ANS := EOL.MK:ID
            EXIT
         END
         IF NOT(ID.ONLY) THEN
            READ TREC FROM IOFILE, ID ELSE 
               CALL uLOGGER(LOG.LEVEL, LOG.KEY:"    uFETCH cannot find ID [":ID:"] in ":FILE)
               LOST+=1; CONTINUE
            END
            IF ENC THEN
               * ----------------------------------------------
               * Check every field for encrypted data
               * ----------------------------------------------
               EOA = DCOUNT(TREC, @FM)
               FOR A = 1 TO EOA
                  TMPA = TREC<A>
                  CHK  = TMPA
                  CONVERT GOOD TO '' IN CHK
                  IF CHK # "" THEN
                     EOM = DCOUNT(TMPA, @VM)
                     FOR M = 1 TO EOM
                        TMPM = TMPA<1,M>
                        CHK  = TMPM
                        CONVERT GOOD TO '' IN CHK
                        IF CHK # "" THEN
                           EOS = DCOUNT(TMPM, @SM)
                           FOR S = 1 TO EOS
                              TMPS = TMPM<1,1,S>
                              CONVERT GOOD TO '' IN TMPS
                              IF LEN(TMPS) THEN TREC<A,M,S> = "ENCRYTED"
                           NEXT S
                        END
                     NEXT M
                  END
               NEXT A
            END
            
            PAYLOAD.SIZE += LEN(TREC)
            
            TREC = EREPLACE(TREC, @SM, atSV)
            TREC = EREPLACE(TREC, @VM, atVM)
            TREC = EREPLACE(TREC, @FM, atFM)
            *
            * Mod - char(13)+char(10) found in data at Kiwibank
            *
            CONVERT BAD TO "" IN TREC
            *
            * ----------------------------------------------
            * Encrypt data at source BEFORE transferring
            IF ENC.SRC THEN
               ORIG = TREC
               CALL SR.ENCRYPT(TREC, ORIG)
               ORIG = ""
            END
            * ----------------------------------------------
            LINE = ID:atIM:TREC
            ANS := EOL.MK:LINE
         END ELSE
            ANS := EOL.MK:ID
         END
         EOL.MK = CRLF
      REPEAT
      IF LOST > 0 THEN 
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:" @@ uFETCH ----------------")
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:" @@ uFETCH Lost ":LOST:" records")
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:" @@ uFETCH ----------------")
      END
RETN:
      RELEASE
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:" >> uFETCH    completed    ":SKEY:" - returning the data now")
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:" >> uFETCH**************************************************")
$IFNDEF isRT
      CLOSE
$ENDIF
      RETURN
   END
