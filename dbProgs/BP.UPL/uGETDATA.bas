      PROMPT ""
$INCLUDE I_Prologue
      CLEAR
      THIS.ACT = "DoNotWorryAboutThis"
$IFDEF isRT
      rCMD = SENTENCE
$ELSE
      rCMD = @SENTENCE
$ENDIF
      CMD = CONVERT(' ', @VM, rCMD)
      LOCATE('uGETDATA', CMD, 1; P1) ELSE
         eMESSAGE = 'Invalid uGETDATA command: ':CMD
         GOSUB ABEND
         STOP
      END
      PROC.NO = CMD<1,P1+1>
      DATACT = CMD<1,P1+2>
      NOWAIT = CMD<1,P1+3>
      PROG   = "uGETDATA_":PROC.NO:"_":DATACT
      * ---------------------------------------------------------
      LOG.KEY = PROG:@FM
      LOG.LEVEL=0
      MSG = "Starting: ":TIMEDATE()
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
      *
      sep='|'
$IFDEF isRT
      DYN = "1,1 7,1 ALU"              ; * Reality
$ENDIF
$IFDEF isUV
      DYN='DYNAMIC'                    ; * UniVerse
$ENDIF
$IFDEF isUD
      DYN=' 919,3 DYNAMIC KEYDATA'     ; * UniData 
$ENDIF
      LOADED="LOADED"
* ------------------------------------------------
      FTYP=DYN ; FILENAME='uMASTER' ; GOSUB OPEN..FILE ; uMASTER=HANDLE
      CONTROL = ''
      READ CONTROL FROM uMASTER, PROC.NO ELSE CONTROL = ""
      SOURCE = TRIM(CONTROL<3>)
      TARGET = TRIM(CONTROL<4>)
      CONTROL<5>=99
      STOP.FLAG=CONTROL<10>
      IF STOP.FLAG THEN
         eMESSAGE = "[ END ] - Master STOP flag is set"
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:eMESSAGE)
         GOSUB ABEND
         GO END..PROG
      END
      IF TARGET='' THEN TARGET=SOURCE
      IF TARGET='' THEN
         eMESSAGE = "[ERROR] No control mapping from source to target"
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:eMESSAGE)
         GOSUB ABEND
         GO END..PROG
      END
      MSG = 'Working on ':SOURCE:' from ':DATACT:' being sent to ':TARGET:' table '
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
      USE.FILE = TARGET:'.QF'
      LD = TARGET:'.LOADED'
      TK = TARGET:'.TAKE'
      OPEN 'VOC' TO VOC ELSE
         eMESSAGE = 'Cannot find VOC'
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:eMESSAGE)
         GOSUB ABEND
         GO END..PROG
      END
      VREC = 'Q':@FM:DATACT:@FM:SOURCE
      READV CHK FROM VOC, SOURCE, 1 THEN
         IF THIS.ACT # DATACT THEN
            IF CHK # 'Q' THEN
               eMESSAGE = 'VOC entry for ':SOURCE:' is not a Q-Pointer. Remedy and re-run'
               CALL uLOGGER(LOG.LEVEL, LOG.KEY:eMESSAGE)
               GOSUB ABEND
               GO END..PROG
            END
         END
      END
      WRITE VREC ON VOC, USE.FILE
      CLOSE VOC
* ------------------------------------------------
      FTYP=DYN ; FILENAME=LD ; GOSUB OPEN..FILE ; LOADED=HANDLE
      FTYP=DYN ; FILENAME=TK ; GOSUB OPEN..FILE ; TAKE.THESE=HANDLE
      OPEN USE.FILE TO INFILE ELSE
         eMESSAGE = '[ABORT] Cannot find file definition for ':USE.FILE
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:eMESSAGE)
         GOSUB ABEND
         GO END..PROG
      END
* ------------------------------------------------
      LOOP WHILE CONTROL<5>=99 DO
         CONTROL<5>='01'
         CONTROL<6>=TIME()
         CONTROL<9>=1
         WRITE CONTROL ON uMASTER, PROC.NO
         SEL.STMT = TRIM(CONTROL<1>)
         NSEL.FLG = CONTROL<2>+0
         IF SEL.STMT='' THEN SEL.STMT = 'SELECT ':USE.FILE
         IF NSEL.FLG THEN SEL.STMT = SEL.STMT:@FM:'NSELECT ':LD
         MSG = SEL.STMT
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
$IFDEF isRT
         EXECUTE SEL.STMT CAPTURING OUT RTNLIST sLIST
         SELECTED = DCOUNT(sLIST, @FM)
$ELSE
         EXECUTE SEL.STMT CAPTURING OUT
         SELECTED = @SELECTED
$ENDIF
         IF NOT(SELECTED) THEN
            eMESSAGE = 'No records selected'
            CALL uLOGGER(LOG.LEVEL, LOG.KEY:eMESSAGE)
            GOSUB ABEND
            GO END..PROG
         END
         LSEL = LEN(SELECTED)
         IF LSEL < 10 THEN LSEL=10
         RJST = "R#":LSEL
         zCT=0
         tCT=0
         rCNT=0
         LOOP
            READNEXT ID ELSE EXIT
            rCNT+=1
            CHK=rCNT/1000
            IF INT(CHK)=CHK THEN
               PCT = rCNT / SELECTED
               PCT = OCONV(ICONV(PCT*100, "MD2"), "MD2"):'%'
               MSG = '(':PCT "R#7":') ':rCNT RJST:' of ':SELECTED
               CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
            END
            READ IOREC FROM INFILE, ID ELSE CONTINUE
            ALREADY.TAKEN = 0
            READ CHK FROM TAKE.THESE, ID THEN 
               IF CHK = IOREC THEN ALREADY.TAKEN = 1
            END
            GOSUB CHECK..LOADED
            IF ALREADY.LOADED THEN CONTINUE
            zCT+=1 ; tCT+=1
            IF ALREADY.TAKEN THEN CONTINUE
            KEY = ID
            GOSUB STRIP..CONTROL..CHARS
            WRITE IOREC ON TAKE.THESE, KEY ON ERROR
               eMESSAGE = '[ERROR] Cannot write ':KEY:' on ':TK
               CALL uLOGGER(LOG.LEVEL, LOG.KEY:eMESSAGE)
               GOSUB ABEND
               GO END..PROG
            END
            IF INT(zCT/100) = (zCT/100) THEN
               REASON='Mid loop' ; ZZ=5 ; GOSUB SNORE ; zCT=0
            END
         REPEAT
         MSG = SELECTED:' record(s) checked and taking ':tCT
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
      REPEAT
END..PROG:
      CONTROL<5> = '99'
      CONTROL<9> = '0'
      WRITE CONTROL ON uMASTER, PROC.NO
      CLOSE INFILE
      CLOSE uMASTER
      CLOSE LOADED
      CLOSE TAKE.THESE
      MSG = '-------------------------[ END ] --------------------------'
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
      PRINT MSG
      STOP
* ------------------------------------------------
STRIP..CONTROL..CHARS:
      FOR IZ = 1 TO 251
         IF (IZ<32 OR IZ>127) THEN
            IOREC = CONVERT(CHAR(IZ), "", IOREC)
         END
      NEXT IZ
      RETURN
* ------------------------------------------------
OPEN..FILE:
      OK=0 ; TRY.CNT=0
      LOOP UNTIL OK DO
         OPEN FILENAME TO HANDLE THEN
            OK=1
         END ELSE
            TRY.CNT+=1
            IF TRY.CNT>2 THEN
               eMESSAGE = '[ABORT] Cannot create ':FILENAME
               CALL uLOGGER(LOG.LEVEL, LOG.KEY:eMESSAGE)
               GOSUB ABEND
               STOP
            END
            EXE = 'CREATE.FILE ':FILENAME:' ':FTYP
            CALL uLOGGER(LOG.LEVEL, LOG.KEY:EXE)
            EXECUTE EXE
         END
      REPEAT
      IF TRY.CNT > 2 THEN OK=0
      RETURN
* ------------------------------------------------
SNORE:
      IF NOWAIT = '' THEN
         SLEEP ZZ
      END
      READV STOP.FLAG FROM uMASTER, PROC.NO, 10 ELSE STOP.FLAG=0
      IF STOP.FLAG THEN
         eMESSAGE = "[ END ] - Master STOP flag is set"
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:eMESSAGE)
         GOSUB ABEND
         STOP
      END
      RETURN
* ------------------------------------------------
CHECK..LOADED:
      ALREADY.LOADED=1
      RVAL = DIGEST("MD5", IOREC, 1, HASH)
      HASH = DOWNCASE(OCONV(HASH, "MX0C"))
      READ CSUM FROM LOADED, ID ELSE CSUM=''
      IF NOT(RVAL) AND (HASH#CSUM) THEN
         ALREADY.LOADED=0
      END
      RETURN
* ------------------------------------------------
ABEND:
      *** PRINT eMESSAGE
      CONTROL<5>=99
      CONTROL<9>=0
      WRITE CONTROL ON uMASTER, PROC.NO
      RETURN
* ------------------------------------------------
   END
