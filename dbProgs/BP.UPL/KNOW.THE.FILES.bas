      *
$INCLUDE I_Prologue
$IFDEF isUV
      CMD = @SENTENCE
$ENDIF
$IFDEF isUD
      CMD = @SENTENCE
$ENDIF
$IFDEF isRT
      CMD = SENTENCE()
$ENDIF
      PRECISION 9
      IF UNASSIGNED (DBT) OR DBT=0 THEN  EXECUTE "CLEAR.COMMON"
      PROG = "KNOW.THE.FILES"
      vSEL = "SELECT VOC WITH F1 LIKE F... OR WITH F1 LIKE Q..."
      vLCK = "LIST.READU"
      fSEL = "SELECT uFILES"
      ERR  = ""
      CALL SR.OPEN.CREATE (ERR, "uFILES", "DYNAMIC", uFILES)
      IF ERR # "" THEN STOP "ERROR condition: ":ERR
      WRITE "" ON uFILES, PROG:"-STOP"
      CALL SR.FILE.OPEN (ERR, "BP.UPL", BP.UPL)
      IF ERR # "" THEN PRINT ERR; RETURN
      CALL SR.FILE.OPEN (ERR, "VOC", VOC)
      IF ERR # "" THEN PRINT ERR; RETURN
      *
      SEP = " "
      INCMD = CMD
      CMD   = ""
      CALL SR.GET.INSTRINGS (RTN.STRING , INCMD , SEP , CMD)
      CMD = CONVERT(@SM, @FM, CMD)
      IF CMD<1> = PROG THEN CMD = DELETE(CMD, 1, 0, 0)
      *
      EXECUTE "WHO" CAPTURING JUNK
      nSPC   = DCOUNT(JUNK, " ")
      WHO    = FIELD(JUNK<1>, " ", nSPC)
      WHOAMI = FIELD(JUNK<1>, " ", 2)
      *
      DIM REC.ARR(10); MAT REC.ARR = ""
      EQU fSIZE   TO REC.ARR(1)
      EQU FILE.ID TO REC.ARR(2)
      EQU DATACT  TO REC.ARR(3)
      EQU tlACC   TO REC.ARR(4)
      EQU dlACC   TO REC.ARR(5)
      EQU tlMOD   TO REC.ARR(6)
      EQU dlMOD   TO REC.ARR(7)
      *
      BEGIN CASE
         CASE CMD<1> = "VOC"
            WRITE DATE():@FM:TIME() ON uFILES, PROG:"-VOC"
            GOSUB DO..THE..VOC
         CASE CMD<1> = "READU"
            WRITE DATE():@FM:TIME() ON uFILES, PROG:"-READU"
            GOSUB DO..THE..READU
         CASE 1
            WRITE DATE():@FM:TIME() ON uFILES, PROG
            GOSUB MANAGE..FILES
      END CASE
      STOP
      * ------------------------------------------------------------
MANAGE..FILES:
      *
      iSTARTED = 0
      LOOP
         READ STP.SW1 FROM BP.UPL, "STOP"       ELSE STP.SW1=""
         READ STP.SW2 FROM uFILES, PROG:"-STOP" ELSE STP.SW2=""
         IF STP.SW1 = "stop" THEN RETURN
         IF STP.SW2 = "stop" THEN RETURN
         *
         IF iSTARTED # DATE() THEN
            * -------- Stop existing phantoms ----------
            WRITE "stop" ON uFILES, PROG:"-STOP"
            RQM; RQM; RQM
            RQM; RQM; RQM
            WRITE ""     ON uFILES, PROG:"-STOP"
            * -------- Start new phantoms --------------
            EXE = "PHANTOM ":PROG:" READU"
            EXECUTE EXE CAPTURING JUNK
            RQM; RQM; RQM
            EXE = "PHANTOM ":PROG:" VOC"
            EXECUTE EXE CAPTURING JUNK
            iSTARTED = DATE()
         END ELSE
            RQM; RQM; RQM
         END
         *
      REPEAT
      *
      RETURN
      *
      * ------------------------------------------------------------
      *
DO..THE..VOC:*
      iSTARTED = 0
      *
      LOOP
         EXECUTE vSEL CAPTURING JUNK
         LOOP
            READ STP.SW1 FROM BP.UPL, "STOP"       ELSE STP.SW1=""
            READ STP.SW2 FROM uFILES, PROG:"-STOP" ELSE STP.SW2=""
            IF STP.SW1 = "stop" THEN RETURN
            IF STP.SW2 = "stop" THEN RETURN
            *
            MAT REC.ARR = ""
            READNEXT fID ELSE EXIT
            FILE.ID = fID
            OPEN FILE.ID TO JUNKIO  ELSE PRINT "Open ERR: ":fID; CONTINUE
            STATUS VAR FROM JUNKIO  ELSE PRINT "Stat ERR: ":fID; CONTINUE
            READ vREC FROM VOC, fID ELSE PRINT "Read ERR: ":fID; CONTINUE
            IF vREC<1>[1,1] = "F" THEN DACT=WHOAMI ELSE DACT=vREC<2>
            IF DACT = FILE.ID THEN DACT = WHOAMI
            IF DACT = "" THEN DATACT = WHOAMI ELSE DATACT = DACT
            fSIZE = VAR<6>
            iNODE = VAR<10>
            tlACC = VAR<13>
            dlACC = VAR<14>
            tlMOD = VAR<15>
            dlMOD = VAR<16>
            MATWRITE REC.ARR ON uFILES, iNODE
            RQM; RQM
         REPEAT
         RQM; RQM; RQM; RQM; RQM; RQM
      REPEAT
      RETURN
      *
      * ------------------------------------------------------------
DO..THE..READU:
      *
      ERR  = ""
      CALL SR.OPEN.CREATE (ERR, "uRESERVED", "DYNAMIC", uRESERVED)
      IF ERR # "" THEN 
         PRINT "ERROR condition: ":ERR
         RETURN
      END
      CALL SR.OPEN.CREATE (ERR, "uRESERVED.INDEX", "DYNAMIC", uRESINDEX)
      IF ERR # "" THEN 
         PRINT "ERROR condition: ":ERR
         RETURN
      END
      *
      DIM RES.ARR(10); MAT RES.ARR = ""
      EQU GRPID   TO RES.ARR(1)
      EQU CORID   TO RES.ARR(2)
      EQU MSGID   TO RES.ARR(3)
      EQU DTMIN   TO RES.ARR(4)
      EQU BEFOR   TO RES.ARR(5)
      EQU AFTER   TO RES.ARR(6)
      *
      DIM RES.IDX(10); MAT RES.IDX = ""
      *
      iSTARTED = 0
      *
      WATCHER = ""
      LEGACY  = "READU-SYS"
      L.CORID = "Legacy-systems"
      L.MSGID = "Legacy-systems"
      L.DTMIN = 0
      L.BEFOR = ""
      L.AFTER = ""
      *
      LOOP
         READ STP.SW1 FROM BP.UPL, "STOP"       ELSE STP.SW1=""
         READ STP.SW2 FROM uFILES, PROG:"-STOP" ELSE STP.SW2=""
         IF STP.SW1 = "stop" THEN RETURN
         IF STP.SW2 = "stop" THEN RETURN
         *
         EXECUTE vLCK CAPTURING LINES
         LNCT = DCOUNT(LINES, @FM)
         THIS.LN = 0
         NEWLIST = ""
         *
         CHK = OCONV(LINES, "MCN")
         IF NUM(CHK) AND CHK#"" THEN
            LOOP
               THIS.LN += 1
            WHILE THIS.LN < LNCT DO
               *
               LINE = TRIM(LINES<THIS.LN>)
               IF LINE = "" THEN CONTINUE
               CHK = OCONV(LINE, "MCN")
               IF NUM(CHK) AND CHK#"" THEN
                  iNODE = FIELD(LINE, " ", 2)
                  USER  = FIELD(LINE, " ", 8)
                  ITEM  = FIELD(LINE, " ", 9)
                  READ ufREC FROM uFILES, iNODE ELSE CONTINUE
                  FILID = ufREC<2>
                  DATACT= ufREC<3>
                  RESKEY= DATACT:"_":FILID:"_":ITEM
                  * ----------- Most variables are EQU --------------
                  MATREADU RES.ARR FROM uRESERVED, RESKEY ELSE MAT RES.ARR = ""
                  GRPID = LEGACY
                  MATREADU RES.IDX FROM uRESINDEX, GRPID  ELSE MAT RES.IDX = ""
                  *
                  CORID = L.CORID
                  MSGID = L.MSGID
                  DTMIN = L.DTMIN
                  BEFOR = L.BEFOR
                  AFTER = L.AFTER
                  tBEGN = ""
                  tWAIT = ""
                  MATWRITE RES.ARR ON uRESERVED, RESKEY ELSE NULL
                  LOCATE(RESKEY, RES.IDX(1)<1,1>; POS) ELSE
                     RES.IDX(1)<1,-1> = RESKEY
                  END
                  MATWRITE RES.IDX ON uRESINDEX, GRPID  ELSE NULL
                  WATCHER<-1> = RESKEY
                  NEWLIST<-1> = RESKEY
               END
               RQM
            REPEAT
         END
         * remove locks which have been released ----------------
         EOW = DCOUNT(WATCHER, @FM)
         FOR I = 1 TO EOW
            THIS.LCK = WATCHER<I>
            IF THIS.LCK = "" THEN CONTINUE
            LOCATE(THIS.LCK ,NEWLIST; FND) ELSE
               IF THIS.LCK = "" THEN CONTINUE
               READU wREC FROM uRESERVED, THIS.LCK ELSE CONTINUE
               READU xREC FROM uRESINDEX, wREC<1>  ELSE xREC=""
               LOCATE(THIS.LCK, xREC<1,1>; POS) THEN
                  xREC = DELETE(xREC, 1, POS, 0)
               END
               IF xREC<1> = "" THEN
                  DELETE  uRESINDEX, wREC<1>
                  RELEASE uRESINDEX, wREC<1>
               END ELSE
                  WRITE xREC ON uRESINDEX, wREC<1> ELSE NULL
               END
               DELETE  uRESERVED, THIS.LCK
               RELEASE uRESERVED, THIS.LCK
               *
               WATCHER = DELETE(WATCHER, I, 0, 0)
               I -= 1
            END
         NEXT I
         RQM; RQM
      REPEAT
      RETURN
      *
   END
