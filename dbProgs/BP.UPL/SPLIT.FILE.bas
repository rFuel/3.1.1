$INCLUDE I_Prologue
      *
      *  Synopsis:
      *     usage: SPLIT.FILE {file} {#groups} {saved-list} {verbose YES/NO}
      *
      * ----------------------------------------------------------------------
      IF MEMORY.VARS(1) =  "" THEN MEMORY.VARS(1) = "uplLOG"
      LOG.KEY = MEMORY.VARS(1):@FM
      LOG.LEVEL = 3
      ERR = ""
      MSG = "SPLIT.FILE starting ======================================="
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
      UPL.LOGGING = 0
      INF.LOGGING = 0
      MAT sockPROPS = ""
      CALL SR.FILE.OPEN(ERR,"UPL.CONTROL", UPL.CONTROL)
      CALL SR.FILE.OPEN(ERR,"BP.UPL", BP.UPL)
      CALL SR.FILE.OPEN(ERR,"VOC", VOC)
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
      *
      PROMPT ""
      CRLF = CHAR(13):CHAR(10)
$IFDEF isUV
      CMD = @SENTENCE:" @ @ @ @ @"
$ENDIF
$IFDEF isUD
      CMD = @SENTENCE:" @ @ @ @ @"
$ENDIF
$IFDEF isRT
      CMD = SENTENCE:" @ @ @ @ @"
$ENDIF
      CONVERT " " TO @FM IN CMD
      LOCATE("SPLIT.FILE", CMD; POS) ELSE STOP "What command?"
      FILE = CMD<POS+1>
      GRPS = CMD<POS+2>
      SLST = CMD<POS+3>
      VBSE = CMD<POS+4>
      BEGIN CASE
         CASE FILE = "@" ; FILE = ""
         CASE GRPS = "@" ; GRPS = ""
         CASE SLST = "@" ; SLST=""
         CASE VBSE = "@" ; VBSE=""
      END CASE
      PROG = "SPLIT.FILE (f=":FILE:") (g=":GRPS:") (s=":SLST:") (v=":VBSE:")"
      *
      IF (GRPS+0 # GRPS) OR (INT(GRPS) # GRPS) THEN
         MSG = PROG:":: [":GRPS:"] must be an integer"
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
         STOP 
      END
      IF GRPS < 2 THEN
         MSG = PROG:":: Groups should be more than 1"
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
         STOP 
      END
      IF SLST = "" THEN
         MSG = PROG:":: Save-List name cannot be empty"
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
         STOP 
      END
      IF VBSE = "YES" THEN VBSE=1 ELSE VBSE=0
      READ DBT FROM BP.UPL, "DBT" ELSE DBT="UV"
      READ rqm FROM BP.UPL, "RQM" ELSE rqm=5000
      SL="THROW-ERROR"
      IF DBT="UV" THEN SL = "&SAVEDLISTS&"
      IF DBT="UD" THEN SL = "SAVEDLISTS"
      *
      IF (INDEX(SLST, "*", 1)) THEN
         SL   = FIELD(SLST, "*", 1)
         SLST = FIELD(SLST, "*", 2)
      END
      *
      ERR=""
      oFNAME = FILE; GOSUB OPEN..FILE; IF ERR # "" THEN STOP ELSE IOFILE = fHANDLE
      oFNAME = SL  ; GOSUB OPEN..FILE; IF ERR # "" THEN STOP ELSE SLISTS = fHANDLE
      *
      ARRAY = ""
      MSG = PROG:":: Clean-up old SL items"
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
      EXE = "SELECT ":SL:" WITH @ID LIKE ":SLST:"..." 
      MSG = PROG:":: Execute: ":EXE
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
      EXECUTE EXE CAPTURING JUNK
      MSG = PROG:":: Result : ":JUNK
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
      MSG = PROG:":: Deleting old SL items"
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
      CT=0
      LOOP
         READNEXT SLKEY ELSE EXIT
         DELETE SLISTS, SLKEY
         CT+=1
      REPEAT
      IF CT>0 THEN
         MSG = PROG:":: ":CT:" items deleted"
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
      END
      *
      PROG = "SPLIT.FILE (":FILE:")"
$IFDEF isRT
      DIM FILES(250)
$ELSE
      DIM FILES(GRPS)
$ENDIF
      *
      * Create recieving SL items for writeseq updates
      *
      REAL.FILE = ""
      MAT FILES=""
      FOR F = 1 TO GRPS
         EXTN= ("000":F) "R#3"
         KEY = SLST:EXTN
         WRITE "" ON SLISTS, KEY
         OPENSEQ SL, KEY TO FILES(F) THEN
            MSG = PROG:":: item ":KEY:" is open and ready for data"
            CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
         END ELSE
            MSG = PROG:":: FATAL :: OPENSEQ failure on [":KEY:"]"
            CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
            STOP
         END
      NEXT F
      CLOSE SLISTS
      *
      MSG = PROG:":: Processing ":FILE:" now"
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
      RESET = 1
      GOSUB FLUSH..DATA
      RESET = 0
      *
      * Select items to process - all OR a specific select-statement.
      *
      SEL = ""
      IF FILEINFO(UPL.CONTROL, 0) THEN
         READ FILE.CONTROL FROM UPL.CONTROL, FILE THEN
            * Before this is run, there was a SET-FILE executed
            * and this created a q-pointer as "FILE".
            * Using this FILE allows me to have select stmts for 
            * any sub-set of any file.
            IF FILE.CONTROL<1> # "" THEN
               SEL = "SELECT ":FILE:" ":FILE.CONTROL<1>
            END
         END
      END
      IF SEL = "" THEN
         MSG = PROG:":: selecting ":FILE
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
         SELECT IOFILE
      END ELSE
         MSG = PROG:":: selecting ":SEL
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
         EXECUTE SEL CAPTURING JUNK
         SELECTED = @SELECTED
         MSG = PROG:":: selected ":SELECTED:" records."
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
      END
      *
      * --------------------------------------------------------------
      *
      LX=0
      NN=0
      CT=0
      GR=0
      SHOW=0
      MKR = ""
      LOOP
         READNEXT ID ELSE EXIT
         IGR1 = GR/10000
         IGR2 = INT(IGR1)
         IF IGR1 = IGR2 THEN
            MSG = PROG:":: ":OCONV(GR, "MD0,"):" records processed"
            CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
***         SHOW = 0
         END
         READ CHK FROM IOFILE, ID ELSE CONTINUE
         * ------------------------------------------------------------
         NN+=1; CT+=1; GR+=1
         IF NN > GRPS THEN NN = 1
         ARRAY<NN> := MKR<NN>:ID  ;  MKR<NN> = CRLF
         * ------------------------------------------------------------
         LX += LEN(ID)
         IF LX => 10000 THEN 
            GOSUB FLUSH..DATA ; LX = 0
         END
         IF CT=>rqm THEN
            MSG = PROG:":: RQM pause <========================="
            CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
            RQM; RQM; RQM; CT=0
         END
      REPEAT
      MSG = PROG:":: ":GR:" records processed."
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
      GOSUB FLUSH..DATA
      GOSUB CHECK..LISTS
      MSG = PROG:":: Finished =================================================="
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
      FOR I = 1 TO GRPS
         WEOFSEQ FILES(I)
         CLOSESEQ FILES(I)
      NEXT I
      STOP
*
* -----------------------------------------------------------------------
*
FLUSH..DATA:
      FOR EXT= 1 TO GRPS
         DAT = ARRAY<EXT>
***      IF DAT#"" THEN REC=DAT ELSE REC=""
$IFDEF isRT
         WRITESEQ DAT TO FILES(EXT) ELSE 
            fn = FILEPATH(FILES(EXT))<4>
            MSG = PROG:":: WRITESEQ error on ":fn
            CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
            STOP
         END
$ELSE
         WRITESEQ DAT TO FILES(EXT) ELSE 
            MSG = PROG:":: WRITESEQ error on ":FILEINFO(FILES(EXT), 1)
            CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
            STOP
         END
$ENDIF
      NEXT EXT
      MKR = ""
      ARRAY=""
      READ STP FROM BP.UPL,"STOP" ELSE STP=""
      IF STP="stop" THEN 
         MSG = PROG:":: STOP switch set ON. Stopping now"
         CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
         STOP
      END
      READ rqm FROM BP.UPL, "RQM" ELSE rqm=5000
      RETURN
*
OPEN..FILE:
      MSG = PROG:":: Opening ":oFNAME
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
      CALL SR.FILE.OPEN(ERR, oFNAME, fHANDLE)
      MSG = PROG:":: "
      IF ERR="" THEN MSG := "PASS" ELSE MSG := "FAIL"
      CALL uLOGGER(LOG.LEVEL, LOG.KEY:MSG)
      RETURN
*
CHECK..LISTS:
      oFNAME = SL  ; GOSUB OPEN..FILE; IF ERR # "" THEN STOP ELSE SLISTS = fHANDLE
      FOR F = 1 TO GRPS
         EXTN= ("000":F) "R#3"
         KEY = SLST:EXTN
         READ DAT FROM SLISTS, KEY ELSE CONTINUE
         hasCHANGED = 0
         LOOP
            CHK = DAT<1>
         WHILE TRIM(CHK) = "" DO
            DAT = DELETE(DAT, 1, 0, 0)
            hasCHANGED = 1
            IF DAT = "" THEN EXIT
         REPEAT
         IF hasCHANGED THEN
            WRITE DAT ON SLISTS, KEY
         END
      NEXT F
      RETURN
   END
