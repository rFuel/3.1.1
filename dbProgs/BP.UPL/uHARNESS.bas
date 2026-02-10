      * Integrate external interface with internal micro-services
$INCLUDE I_Prologue
      
      GOSUB INITIALISE
      GOSUB PROCESS
      GO END..PROGRAM
      * --------------------------------------------------------------------- *
INITIALISE:
      MEMORY.VARS(1) = "uHARNESS"
      LOG.KEY = MEMORY.VARS(1) : @FM
      ** INCMD = @SENTENCE
      INCMD = SENTENCE()
      XCMD = INCMD
      CONVERT " " TO @FM IN XCMD
      LOCATE("uHARNESS", XCMD; POS) ELSE STOP
      POS -= 1
      FOR X = 1 TO POS
         XCMD = DELETE(XCMD, 1, 0, 0)
      NEXT X
$IFDEF isRT
      IF (XCMD<2>[1,1] = '"') THEN
         XCMD<2> = XCMD<2>[2, LEN(XCMD<2>)]
         POS = DCOUNT(XCMD, @FM)
         IF XCMD<POS>[LEN(XCMD<POS>), 1] = '"' THEN
            XCMD<POS> = XCMD<POS>[1, LEN(XCMD<POS>)-1]
         END
      END
$ENDIF
      CONVERT @FM TO " " IN XCMD
      INCMD = XCMD
      MSGHEADER = "[uHARNESS] started --------------------------------------------"
      CALL SR.GET.PROPERTY ("lock.wait", ans)
      IF NUM(ans) AND ans # "" THEN LOCK.WAIT=ans ELSE LOCK.WAIT = 3
      IF LOCK.WAIT > 0 THEN LOCK.WAIT -= 0.5
$IFDEF isRT
      MAX = DCOUNT(FNAMES, @FM)
$ELSE
      MAX = INMAT(FHANDLES)<1,1,1>
$ENDIF
      IF (FIELD(INCMD, " ", 2)[1,1]="!") THEN
         IF MEMORY.VARS(1) = "" THEN MEMORY.VARS(1) = "uHARNESS"
         LX = INDEX(INCMD, " ", 1)+2
         INCMD = INCMD[LX, LEN(INCMD)]
         RUN.AS.PHANTOM=0
      END ELSE
$IFDEF isRT
         EXECUTE "NC.RESET usock rFuel" CAPTURING JUNK
$ELSE
         CLEAR COMMON
$ENDIF
         FNAMES = ""
         MAT FHANDLES = ""
         MAT MEMORY.VARS = ""
         DIM CALL.STRINGS(20) 
         BP.UPL = ""
         uREQUESTS = ""
         uRESPONSES= ""
         sRESPONSES= ""
         COMO = ""
         RETURN.CODES = ""
         uCATALOG = ""
         UPL.LOGGING = 0
         INF.LOGGING = 0
         sockDBG=0
         MAT sockPROPS = ""
         MEMORY.VARS(1) = INCMD
         CONVERT " " TO "-" IN MEMORY.VARS(1)
         RUN.AS.PHANTOM=1
      END
      IF TRIM(MEMORY.VARS(1)) = "" THEN MEMORY.VARS(1) = "uHARNESS"
      LOG.KEY = MEMORY.VARS(1) : @FM
      CALL SR.OPEN.CREATE(ERR, "BP.UPL", "19", BP.UPL)
      IF ERR # "" THEN
         DBT = "UV"
      END ELSE
         READ DBT FROM BP.UPL, "DBT" ELSE DBT = "UV"
         READ PARAMS FROM BP.UPL, "properties" ELSE PARAMS = ""
         MATPARSE sockPROPS FROM PARAMS
         pAns = ""; CALL SR.GET.PROPERTY("upl.logging", pAns)  ; UPL.LOGGING = pAns
         pAns = ""; CALL SR.GET.PROPERTY("inf.logging", pAns)  ; INF.LOGGING = pAns
         pAns = ""; CALL SR.GET.PROPERTY("sel.loopwait", pAns) ; LOOP.WAIT = OCONV(ICONV(pAns, "MD0"), "MD0")
         IF LOOP.WAIT < 100 THEN LOOP.WAIT = 100
$IFDEF isRT
         LOOP.WAIT = LOOP.WAIT / 1000
$ENDIF
         IF PARAMS="" THEN
            PARAMS<1> = UPL.LOGGING
            PARAMS<2> = INF.LOGGING
         END
         WRITE DBT ON BP.UPL, "DBT"
         WRITE PARAMS ON BP.UPL, "properties"
      END
      PRECISION 9
      IF INF.LOGGING THEN
         CALL SR.OPEN.CREATE(ERR, "uLOG", "19", COMO)
         IF ERR # "" THEN
            MSG = "uLOG is missing from FHANDLES - may be user permissions"
            PRINT MSG
            STOP
         END
         TRIES=1
         LOOP WHILE TRIES < 3 DO
            IF LEN(TRIM(LOG.KEY<1>)) < 2 THEN LOG.KEY<1> = "uHARNESS"
            fname = "uLOG,":LOG.KEY<1>
            OPENSEQ "uLOG", LOG.KEY<1> TO IOHANDLE THEN
               LOCATE(fname, FNAMES; idx) ELSE idx = DCOUNT(FNAMES, @FM)
               FNAMES<idx> = fname
               FHANDLES(idx) = IOHANDLE
               EXIT
            END ELSE
               IF TRIES = 2 THEN
                  PRINT "Cannot openseq uLOG,":LOG.KEY<1>
$IFNDEF isRT
                  CLOSE
$ENDIF
                  STOP
               END
               WRITE "" ON COMO, LOG.KEY<1>
            END
            TRIES+=1
         REPEAT
      END
      IF INF.LOGGING AND RUN.AS.PHANTOM THEN CALL uLOGGER(1, LOG.KEY:" Started /\/\/\/\/\/\/\/\/\")
      ARR = ""
      EQU RTN.MESSAGE TO ARR<1>
      CALL SR.GET.INSTRINGS (RTN.STRING , TRIM(INCMD) , " " , CMD)
      THIS.PORTAL = ""
      EOI = DCOUNT(CMD, @FM)
      FOR I = 1 TO EOI
         IF UPCASE(CMD<I>)[1,5] = "PORT=" THEN
            THIS.PORTAL = CMD<I>[6,LEN(CMD<I>)]
            EXIT
         END
      NEXT I
      GOSUB OPEN..FILES
      RQM.YN = 1
      HBEAT.YN = 0
      HBEAT.BASE = 120
      HBEAT.RESET = 1200
      HBEAT.CNT = HBEAT.BASE
$IFDEF isRT
      IF RUN.AS.PHANTOM THEN RQM LOOP.WAIT
$ELSE
      IF RUN.AS.PHANTOM THEN NAP LOOP.WAIT
$ENDIF
      SEP = "*"
      PING= "PING"
      RETURN
      *
      
OPEN..FILES:
      * --------------------------------------------------------------------- *
      * Get required files ::                                                 *
      * --------------------------------------------------------------------- *
      
      CALL SR.FILE.OPEN(ERR, "RETURN.CODES", RETURN.CODES)
      IF ERR # "" THEN 
         EXECUTE "uLOAD.RETURN.CODES"
         CALL SR.FILE.OPEN(ERR, "RETURN.CODES", RETURN.CODES)
         IF ERR # "" THEN GO END..PROGRAM
      END
      
      CALL SR.OPEN.CREATE(ERR, "uREQUESTS", "19", uREQUESTS)
      IF ERR # "" THEN GO END..PROGRAM
      
      CALL SR.OPEN.CREATE(ERR, "uRESPONSES", "19", uRESPONSES)
      IF ERR # "" THEN GO END..PROGRAM
      
      CALL SR.OPEN.CREATE(ERR, "sRESPONSES", "19", sRESPONSES)
      IF ERR # "" THEN GO END..PROGRAM
      
      CALL SR.OPEN.CREATE(ERR, "uCATALOG", "DYNAMIC", uCATALOG)
      IF ERR # "" THEN GO END..PROGRAM
      *
      RETURN
      *
      * ***********************************************************************
      *
PROCESS:
      * --------------------------------------------------------------------- *
      * Endless Loop for request handling ::                                  *
      * --------------------------------------------------------------------- *
      IF INF.LOGGING AND RUN.AS.PHANTOM THEN CALL uLOGGER(1, LOG.KEY:" Processing")
      STOP.ME = 0
      NOTHING.TODO= 0
$IFDEF isRT
      IF sRESPONSES # "" THEN CLOSE sRESPONSES
$ELSE
      CLOSE sRESPONSES
$ENDIF
      GOSUB LOOP..CONTROL
      LOOP UNTIL STOP.ME
         * ------------------------------------------------------------------ *
         * files handles MAY close automatically on "time-out"                *
         * recheck validity before each loop                                  *
         * ------------------------------------------------------------------ *
$IFDEF isRT
         IF uREQUESTS  = "" THEN GOSUB OPEN..FILES
         IF uRESPONSES = "" THEN GOSUB OPEN..FILES
$ELSE
         IF NOT(FILEINFO(uREQUESTS,0))  THEN GOSUB OPEN..FILES
         IF NOT(FILEINFO(uRESPONSES,0)) THEN GOSUB OPEN..FILES
$ENDIF
         * ------------------------------------------------------------------ *
***      IF (RUN.AS.PHANTOM) THEN
            IF (THIS.PORTAL # "") THEN
$IFDEF isRT
               EXE = 'SELECT uREQUESTS = "[':THIS.PORTAL:']"'
               EXECUTE EXE,//SELECT. > SEL.LIST CAPTURING JUNK
$ELSE
               EXE = "SELECT uREQUESTS LIKE ...":THIS.PORTAL:"..."
               EXECUTE EXE CAPTURING JUNK
$ENDIF
            END ELSE
               SELECT uREQUESTS
            END
***      END
         LOOP.CNT=0;
         * --------------------------------------------------------------------- *
         * Can have any number of uHARNESS phantoms running so do not abort when *
         * something unexpected happens - e.g. a record is "missing"             *
         * --------------------------------------------------------------------- *
         * IF NOT(INF.LOGGING) THEN CRT "No INF.LOGGING"
         * IF NOT(UPL.LOGGING) THEN CRT "No UPL.LOGGING"
         LOOP
            MEMORY.VARS(2) = "" ; * Reset CorrelationID to null.
            MEMORY.VARS(3) = "" ; * Reset ReplyTo       to null.
$IFDEF isRT
            READNEXT ID FROM SEL.LIST ELSE
               LOG.MSG = "[uHARNESS] finished --------------------------------------"
               IF INF.LOGGING THEN CALL uLOGGER(3, LOG.KEY:LOG.MSG)
               STOP
            END
$ELSE
            READNEXT ID ELSE 
***            IF RUN.AS.PHANTOM THEN EXIT ELSE ID="uDIRECT"
               IF RUN.AS.PHANTOM THEN 
                  EXIT 
               END ELSE 
                  LOG.MSG = "[uHARNESS] finished --------------------------------------"
                  IF INF.LOGGING THEN CALL uLOGGER(3, LOG.KEY:LOG.MSG)
                  STOP
               END
            END
$ENDIF
            POSX = DCOUNT(ID, ".")
            EXTN = FIELD(ID, ".", POSX)
            IF EXTN = "temp"  THEN CONTINUE
            IF EXTN = "admin" THEN CONTINUE
            IF INF.LOGGING THEN CALL uLOGGER(3, LOG.KEY:MSGHEADER)
            IF INF.LOGGING THEN CALL uLOGGER(3, LOG.KEY:"Found new MessageID  [":ID:"]")
            * --------------------------------------------------------------- *
            IF NOT(RUN.AS.PHANTOM) AND THIS.PORTAL="" THEN
               STX = TIME()
               RESPONSE = ""
               INSTANT = 0
               RQM.YN = 0
               GOSUB HANDLE..REQUEST
               ETX = TIME()
               DIFF= ETX - STX
               IF INF.LOGGING THEN CALL uLOGGER(3, LOG.KEY:"Finished message [":ID:"] in ":DIFF:" seconds")
               RETURN
            END
            READU REQUEST FROM uREQUESTS, ID LOCKED CONTINUE THEN
               DELETE uREQUESTS, ID
               RESP.ID = ID
               IF THIS.PORTAL # "" THEN 
                  * uRestResponder filters on CorrelationID - excludes Portal #
                  RESP.ID = RESP.ID[INDEX(RESP.ID, "_", 1)+1, LEN(RESP.ID)]
               END
               STX = TIME()
               NOTHING.TODO = 0
               LOOP.CNT += 1
               HBEAT.YN = 0
               RQM.YN = 0
               RESPONSE = ""
               INSTANT = (REQUEST<1> = "{socket}" OR REQUEST<1> = "{mount}")
               GOSUB HANDLE..REQUEST
               RELEASE uREQUESTS, ID
               IF INSTANT THEN RESPONSE = REPLY
               IF INSTANT THEN
                  OPEN "sRESPONSES" to sRESPONSES ELSE STOP "sRESPONSES is not available."
                  WRITE RESPONSE ON sRESPONSES, RESP.ID
                  RELEASE sRESPONSES, ID
                  CLOSE sRESPONSES
               END ELSE
                  WRITE RESPONSE ON uRESPONSES, RESP.ID
               END
               ETX = TIME()
               DIFF= ETX - STX
               IF INF.LOGGING THEN CALL uLOGGER(3, LOG.KEY:"Finished message [":ID:"] in ":DIFF:" seconds")
               IF INF.LOGGING THEN CALL uLOGGER(3, LOG.KEY:"    ")
               GOSUB LOOP..CONTROL
               IF STOP.ME THEN EXIT
            END ELSE
               RELEASE uREQUESTS, ID
            END
         REPEAT
$IFDEF isRT
         IF LOOP.CNT=0 THEN NOTHING.TODO+=1; RQM LOOP.WAIT
$ELSE
         IF LOOP.CNT=0 THEN NOTHING.TODO+=1; NAP LOOP.WAIT
$ENDIF
         GOSUB LOOP..CONTROL
         IF (NOTHING.TODO => HBEAT.RESET) THEN
            IF INF.LOGGING AND NOT(HBEAT.YN) THEN CALL uLOGGER(0, LOG.KEY:"<<heartbeat>> ":NOTHING.TODO:" loops with no data to process")
            HBEAT.YN = 1
            RQM.YN = 1
            RQM
            NOTHING.TODO = 0
         END
      REPEAT
      IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:"STOP switch set on.")
      EXECUTE "CLEARSELECT"
      RETURN
      ************************************************************************
LOOP..CONTROL:
      READ CHKR FROM BP.UPL, "STOP" ELSE CHKR=""
      IF CHKR="stop" THEN STOP.ME = 1
      READ sockDBG FROM BP.UPL, "sockDBG" ELSE sockDBG=""
      IF UPCASE(sockDBG) = "OFF" THEN sockDBG=0 ELSE sockDBG=1
      RETURN
      *
      ************************************************************************
      *
HANDLE..REQUEST:
      IF RUN.AS.PHANTOM THEN 
         INSTR = REQUEST 
      END ELSE 
         IF REQUEST # "" THEN
            INSTR = REQUEST
         END ELSE
            INSTR=EREPLACE(INCMD, "<tm>", @FM)
         END
      END
      IF INSTANT THEN 
         * socket or mount request flagged in request<1>
         STATUS = 200
         REPLY = "" ; REPLY2 = "" ; CORRID = "" ; MSFMT = ""
         GOSUB INSTANT..REQUEST
         MSG = "INSTANT request has been completed"
         GO END..REQUEST
      END
      VPOOL = "X"
      MPOOL = "X"
      DPOOL = "X"
      MPPTY = ""
      REPLY2= "X"
      CORRID= "X"
      MSCAT = "X"
      MSFMT = ""
      * --------------------------------------------------------------------- *
      * Unpack the request data ::                                            *
      * --------------------------------------------------------------------- *
      NBR.ATTRS = DCOUNT(INSTR, @FM)
      DIRECT.CMD = ""
      FOR A = 1 TO NBR.ATTRS
         LINE = INSTR<A>
         IF NOT(RUN.AS.PHANTOM) AND THIS.PORTAL="" AND A = 1 THEN 
            DIRECT.CMD = FIELD(LINE, " ", 1):" "
            LINE = LINE[LEN(DIRECT.CMD)+1, LEN(LINE)]
         END
         TAG  = FIELD(LINE, "=", 1)
         IF (TAG # "") THEN
            LINE = LINE[LEN(TAG)+2, LEN(LINE)]
            TAG := "="
         END
         TMP.MSG = ""
         BEGIN CASE
            CASE TAG = "vpool="
               CHK.BRACES = INDEX(LINE, "<bo>", 1) + INDEX(LINE, "<bc>", 1)
               IF (CHK.BRACES > 0) THEN
                  LINE = EREPLACE(LINE, "<bo>", "{")
                  LINE = EREPLACE(LINE, "<bc>", "}")
               END
               VPOOL = LINE
               CONVERT CHAR(9) TO @VM IN VPOOL  ;* tsv pool of variables
               DIRECT.CMD := "vpool=":VPOOL:"<tm>"
               TMP.MSG = "Processed vpool"
            CASE TAG = "mpool="
               CHK.BRACES = INDEX(LINE, "<bo>", 1) + INDEX(LINE, "<bc>", 1)
               IF (CHK.BRACES > 0) THEN
                  LINE = EREPLACE(LINE, "<bo>", "{")
                  LINE = EREPLACE(LINE, "<bc>", "}")
               END
               MPOOL = LINE
               CONVERT CHAR(9) TO @VM IN MPOOL  ;* tsv pool of db mappings
               DIRECT.CMD := "mpool=":MPOOL:"<tm>"
               TMP.MSG = "Processed mpool"
            CASE TAG = "dpool="
               CHK.BRACES = INDEX(LINE, "<bo>", 1) + INDEX(LINE, "<bc>", 1)
               IF (CHK.BRACES > 0) THEN
                  LINE = EREPLACE(LINE, "<bo>", "{")
                  LINE = EREPLACE(LINE, "<bc>", "}")
               END
               DPOOL = LINE
               CONVERT CHAR(9) TO @VM IN DPOOL  ;* tsv pool of data values
               DIRECT.CMD := "dpool=":DPOOL:"<tm>"
               TMP.MSG = "Processed dpool"
*## s1
            CASE TAG = "props="
               MPPTY = LINE
               CONVERT CHAR(9) TO @VM IN MPPTY
               TMP.MSG = "Processed props ":MPPTY
               GOSUB SET..PROPERTIES
            CASE TAG = "dacct="
               DACCT = LINE
               TMP.MSG = "Processed dacct ":DACCT
               GOSUB SET..POINTER..ACT
*## e1
            CASE TAG = "reply="
               TMP.MSG = "Processed reply"
               REPLY2= LINE                         ;* where to send the response
            CASE TAG = "corrl="
               TMP.MSG = "Processed corrl"
               CORRID= LINE                         ;* correlation ID for integration
            CASE TAG = "mscat="
               TMP.MSG = "Processed mscat"
               MSCAT = LINE                         ;* micro-service ID
            CASE TAG = "msfmt="
               TMP.MSG = "Processed msfmt"
               MSFMT = LINE                         ;* message return format
            CASE 1
               DIRECT.CMD := " ":LINE
         END CASE
      NEXT A
      REPLY = ""
      IF NOT(RUN.AS.PHANTOM) AND THIS.PORTAL="" THEN
         EXECUTE DIRECT.CMD CAPTURING REPLY
         IF REPLY # "" THEN PRINT REPLY
         MSG = ""
         GO END..REQUEST
      END
      BASE = " Malformed micro-service request on [":ID:"]  "
      ** IF VPOOL = "X" THEN REPLY := " - no VPOOL found."
      ** IF MPOOL = "X" THEN REPLY := " - no MPOOL found."
      ** IF DPOOL = "X" THEN REPLY := " - no DPOOL found."
      IF REPLY2= "X" THEN REPLY := " - no REPLY2 found."
      IF CORRID= "X" THEN REPLY := " - no CorrelationID found."
      IF MSCAT = "X" THEN REPLY := " - no Micro-service found."
      IF MSFMT = ""  THEN MSFMT = "xml"
      
      IF REPLY # "" THEN 
         IF INF.LOGGING THEN CALL uLOGGER (1, LOG.KEY:" ":BASE:REPLY)
         REPLY = BASE:REPLY
         GO BUILD..RESPONSE
      END
      MEMORY.VARS(2) = CORRID
      IF INF.LOGGING THEN CALL uLOGGER(3, LOG.KEY:" CorrelationID  [":CORRID:"]")
      
      IF sockDBG THEN
         CALL uLOGGER (1, LOG.KEY:" sockPROPS for this message")
         Z1=1
         FOR Z2 = 1 TO 100
            IF sockPROPS(Z2) # "" THEN
               CALL uLOGGER (1, LOG.KEY:Z1 "R#2":" ":sockPROPS(Z2))
               Z1 += 1
            END
         NEXT Z2
      END
      * --------------------------------------------------------------------- *
      ORDER    = ""        ; * the order in which to execute objects
      SRTNS    = ""        ; * subroutines to call                  
      PRGMS    = ""        ; * programs to execute                  
      REPLY    = ""
      RTN.CODE = ""
      RTN.MSG  = ""
      * --------------------------------------------------------------------- *
      IF INF.LOGGING THEN CALL uLOGGER(5, LOG.KEY:"Setup mscat ":MSCAT)
      
      EXISTS=0 ; END.SW=0
      TMP.MSCAT = MSCAT ; TMP.MSCDETS=""
      LOOP UNTIL END.SW DO
         END.SW = 1
         CALL SR.ITEM.EXISTS (EXISTS, uCATALOG, TMP.MSCAT, VALS, 0)
         BEGIN CASE
            CASE EXISTS=0
               REPLY = MSCAT:" is not a valid uCATALOG"
               IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:" ":REPLY)
               GOSUB BUILD..RESPONSE
               MSG = ""
               GO END..REQUEST
            CASE EXISTS=1
               TMP.MSCDETS<-1> = VALS
               EOS = DCOUNT(TMP.MSCDETS, @FM)
               FOR S = 1 TO EOS
                  msType = UPCASE(FIELD(TMP.MSCDETS<S>, "-",1))
                  IF msType = "UCAT" THEN
                     msName = TRIM(FIELD(TMP.MSCDETS<S>, "-",3))
                     TMP.MSCAT = msName
                     END.SW = 0
                     TMP.MSCDETS = DELETE(TMP.MSCDETS, S)
                     EXIT
                  END
               NEXT S
            CASE EXISTS=2
$IFDEF isRT
               RQM 250
$ELSE
               NAP 250
$ENDIF
               END.SW = 0
         END CASE
      REPEAT
      UNUSED = 0
      LOOP
         TMP.MSCAT = "temp.":CORRID:".":UNUSED
         READU CHK FROM uCATALOG, TMP.MSCAT THEN RELEASE ELSE EXIT
         UNUSED += 1
      REPEAT
      WRITE TMP.MSCDETS ON uCATALOG, TMP.MSCAT
      RELEASE uCATALOG, TMP.MSCAT
      
      MSCAT = TMP.MSCAT
      RTN.CODE = ""
      RTN.MSG = ""
      CALL SR.PREPARE.MSERVICE (RTN.CODE, RTN.MSG, MSCAT, ORDER, SRTNS, PRGMS)
      IF RTN.CODE # "" THEN
         REPLY = "[":RTN.CODE:"] ":RTN.MSG
         IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:" ":REPLY)
         GOSUB BUILD..RESPONSE
         MSG = ""
         GO END..REQUEST
      END
      * --------------------------------------------------------------------- *
      IF INF.LOGGING THEN CALL uLOGGER(9, LOG.KEY:" > Starting execute of ":MSCAT)
      CALL SR.EXECUTE.MSERVICE (REPLY, MSCAT, ORDER, SRTNS, PRGMS, VPOOL, MPOOL, DPOOL)
      IF INF.LOGGING THEN CALL uLOGGER(9, LOG.KEY:" < Finished execute of ":MSCAT)
      
BUILD..RESPONSE:
      
      VALID = (REPLY = "" OR REPLY = 0 OR REPLY[1,3] = "200")
      IF TMP.MSCAT[1,5] = "temp." THEN DELETE uCATALOG, TMP.MSCAT
      IF NOT(VALID) THEN
         STATUS = 500
         IF LEN(REPLY) < 180 THEN
            REPLY = "uHARNESS:: ":REPLY
            IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:REPLY)
         END ELSE
            IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:"Invalid reply - too long to show.")
         END
      END ELSE
         STATUS = "200"
         REPLY = REPLY[INDEX(REPLY, "-", 1)+1, LEN(REPLY)]
      END
      IF UPCASE(MSFMT) = "TEXT" THEN
         RESPONSE = REPLY
      END ELSE
         READ DESC FROM RETURN.CODES, STATUS ELSE DESC = "ReturnCode [":STATUS:"] is missing "
         RESPONSE = '<?xml version="1.0" ?><body><status>':STATUS:'</status><message>'
         RESPONSE:= DESC:'</message><response>':REPLY:'</response></body>'
      END
      RESPONSE:= @FM:REPLY2:@FM:CORRID:@FM:MSFMT
      MSG = "Response Sent to uRESPONSES"
END..REQUEST:
      IF INF.LOGGING AND MSG # "" THEN CALL uLOGGER(5, LOG.KEY:MSG)
      READ PARAMS FROM BP.UPL, "properties" ELSE PARAMS = ""
      MATPARSE sockPROPS FROM PARAMS
      IF sockDBG THEN
         CALL uLOGGER (1, LOG.KEY:" Reset of sockPROPS after the message")
         Z1=1
         FOR Z2 = 1 TO 100
            IF sockPROPS(Z2) # "" THEN
               CALL uLOGGER (1, LOG.KEY:Z1 "R#2":" ":sockPROPS(Z2))
               Z1 += 1
            END
         NEXT Z2
         *
         NBR.HANDLES=0
         FOR Z2 = 1 TO MAX
            IF FNAMES<Z2> # "" THEN NBR.HANDLES +=1 
         NEXT Z2
         CALL uLOGGER (1, LOG.KEY:NBR.HANDLES:" Open file handles ----------------------------------------")
      END
      RETURN
      *
SET..POINTER..ACT:
      DONE = 0
      DAx  = 1
      LOOP WHILE DAx < 100
         IF FIELD(sockPROPS(DAx), "=", 1) = "dacct" THEN 
            DONE = 1
            EXIT
         END
         IF sockPROPS(DAx) = "" THEN 
            sockPROPS(DAx) = "dacct=":DACCT
            DONE = 1
            EXIT
         END
         DAx += 1
      REPEAT
      IF NOT(DONE) THEN
         MSG = "Too many properties, sockPROPS maximum is 100."
         IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:" ":MSG)
         PRINT MSG
         STOP
      END
      RETURN
      
SET..PROPERTIES:
      * Clear ALL previous msg_ entries         
      EMPTY.SLOT=100
      FOR ix = 100 TO 1 STEP -1
         IF sockPROPS(ix) = "" AND ix < EMPTY.SLOT THEN EMPTY.SLOT = ix
      NEXT ix
      *  Set this message's over-rides           
      EOI = DCOUNT(MPPTY, @VM)
      FOR ix = 1 TO EOI
         LNE = MPPTY<1, ix>
         DONE = 0
         LOOP WHILE EMPTY.SLOT < 100
            IF sockPROPS(EMPTY.SLOT) = LNE THEN 
               DONE = 1
               EXIT
            END
            IF sockPROPS(EMPTY.SLOT) = "" THEN 
               sockPROPS(EMPTY.SLOT)=LNE
               DONE = 1
               EXIT
            END
            EMPTY.SLOT += 1
         REPEAT
         IF NOT(DONE) THEN
            MSG = "Too many properties, sockPROPS maximum is 100."
            IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:" ":MSG)
            PRINT MSG
            STOP
         END
      NEXT ix
      IF sockDBG THEN
         CALL uLOGGER (1, LOG.KEY:" Reset of sockPROPS after the message")
         Z1=1
         FOR Z2 = 1 TO 100
            IF sockPROPS(Z2) # "" THEN
               CALL uLOGGER (1, LOG.KEY:Z1 "R#2":" ":sockPROPS(Z2))
               Z1 += 1
            END
         NEXT Z2
         *
         NBR.HANDLES=0
         FOR Z2 = 1 TO MAX
            IF FNAMES<Z2> # "" THEN NBR.HANDLES +=1 
         NEXT Z2
         CALL uLOGGER (1, LOG.KEY:NBR.HANDLES:" Open file handles ----------------------------------------")
      END
      RETURN
      *
INSTANT..REQUEST:
      * call SR.METABASIC ??
      cmd.string = REQUEST
      cmd.string = DELETE(cmd.string,1,0,0)
      CONVERT "{" TO "" IN cmd.string
      CONVERT "}" TO @VM IN cmd.string
      CONVERT "=" TO @SM IN cmd.string
      cmd = cmd.string<1,1>
      cmd.string = DELETE(cmd.string,1,1,0)
      REPLY = "{EOX}"
      MAT CALL.STRINGS = "" 
      CALL.STRINGS(1) = cmd.string
      CALL.STRINGS(2) = LOG.KEY
      BEGIN CASE
         CASE cmd = "STOP"
            STOP
         CASE cmd = "CLF"
            CALL SR.CLF (REPLY, MAT CALL.STRINGS)
         CASE cmd = "RDI"
            CALL SR.RDI (REPLY, MAT CALL.STRINGS)
         CASE cmd = "SAR"
            CALL SR.SELECT.AND.READ (REPLY, MAT CALL.STRINGS)
         CASE cmd = "SRT"
            CALL SR.SRT (REPLY, MAT CALL.STRINGS)
         CASE cmd = "WRI"
            CALL SR.WRI (REPLY, MAT CALL.STRINGS)
         CASE cmd = "EXE"
            CALL SR.EXE (REPLY, MAT CALL.STRINGS)
         CASE 1
            REPLY = "{EOX}"
      END CASE
      RETURN
      *
END..PROGRAM:
      STOP
      */\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\*
   END

