      * Read deltas from Reality transaction log and send to uDELTA.LOG
$INCLUDE I_Prologue
      *
START..PROG:
      PROG = "rTRANLOG"
      MEMORY.VARS(1) = PROG
      LOG.KEY = MEMORY.VARS(1) : @FM
      ZERROR = @FALSE
      THISLOG = ""
      * ----------------------------------------------------- *
      GOSUB INITIALISE
      LASTLOG = LOGFILE
      LOOP UNTIL ZERROR DO
         IF LASTLOG # LOGFILE THEN
            GOSUB PROCESS
            LASTLOG = LOGFILE
         END ELSE
            RQM; RQM; RQM
            SLEEP 5
         END
         GOSUB GET..LOGFILE
      REPEAT
      * ----------------------------------------------------- *
      *
      STOP
      *
      * ********************************************************************* *
INITIALISE:
      OPEN 'uDELTA.LOG' TO DELTAS ELSE 
         LOG.MSG  = "Open failure: uDELTA.LOG"
         CALL uLOGGER(0, LOG.KEY:LOG.MSG)
         ZERROR = @TRUE
         RETURN
      END
      dot = '_' ; SLASH = "/" ; EXTN = ".ulog"
      atIM = "<im>"  ; atTM = "<tm>" ; atKM = "<km>"
      atFM = "<fm>"  ; atVM = "<vm>" ; atSM = "<sm>"
      * ----------------------------------------------------- *
      OPEN "BP.UPL" TO BP.UPL ELSE 
         LOG.MSG  = "Open failure: BP.UPL"
         CALL uLOGGER(0, LOG.KEY:LOG.MSG)
         ZERROR = @TRUE
         RETURN
      END
      READ ENC FROM BP.UPL, "@ENCRYPT" ELSE ENC = ""
      IF ENC # "" THEN ENC = 1 ELSE ENC = 0
      EREC = ""
      GOSUB GET..LOGFILE
      RETURN
      * ********************************************************************* *
PROCESS:
      IF ZERROR THEN RETURN
      *
      * ********************************************************************* *
      * Need;                                                                 *
      *     to understand when to switch log files. At the moment, it is      *
      *     looking at BP.UPL, @TRANLOG for the log file name but there is    *
      *     no way to update that item when the log file changes - by date,   *
      *     size, etc.                                                        *
      * Also;                                                                 *
      *     need to stick on the log file for new records as they happen, so  *
      *     change from readnext else exit to readnext else select and go on  *
      *     this means you have to keep a record of what's been taken!!!      *
      * ********************************************************************* *
      *
      SELECT CLOG TO SELLIST
      WRITE "" ON CLOG,"#FULL-VIEW#";   * Enable the full view feature
      LOOP WHILE READNEXT IID FROM SELLIST
         READ LOGHDR FROM CLOG, IID ELSE CONTINUE
         ACCT = LOGHDR<11>
         FILE = LOGHDR<12>
         ITEM = LOGHDR<13>
         APOS = INDEX(ACCLIST, ACCT, 1)
         FPOS = INDEX(FILLIST, FILE, 1)
         IF ACCT = "" THEN CONTINUE
         IF APOS <=1  THEN CONTINUE
         IF FILE = "" THEN CONTINUE
         IF FPOS <=1  THEN CONTINUE
         *
         EVENT.SOURCE = ACCT:" ":FILE
         LOCATE(EVENT.SOURCE, REGISTER; rfPOS) ELSE CONTINUE
         *
         EOLOG  = LOGHDR<17>
         LPOS   = DELIM.POS(LOGHDR<EOLOG>, "E") + 1
         NREC   = LOGHDR[LPOS, -1]
         READU MT.REC FROM DELTAS, KEY ELSE 
            NREC   = EREPLACE(NREC, @SM, atSM)
            NREC   = EREPLACE(NREC, @VM, atVM)
            NREC   = EREPLACE(NREC, @FM, atFM)
            HDR    = ACCT:atIM:FILE:atIM:ITEM:atIM
            NREC   = HDR:NREC
            IF ENC THEN
               CALL SR.ENCRYPT(EREC, NREC)
               NREC = EREC
               EREC = ""
            END
            WRITE NREC ON DELTAS, KEY
            RELEASE DELTAS, KEY
            EXIT
         END
         RELEASE DELTAS, KEY
         LCK.CNT+=1
      REPEAT
      RETURN
      * ********************************************************************* *
GET..LOGFILE:
      READ PROPS FROM BP.UPL, "properties" ELSE PROPS = ""
      EOI = DCOUNT(PROPS, @FM)
      AFL = ""       ;* AFL - Audit File Location
      ALF = ""       ;* ALF - Audit Log FILE     
      FOR I = 1 TO EOI
         KEY = FIELD(PROPS<I>, "=", 1)
         IF UPCASE(KEY) = "RTAUDLOG" THEN AFL = FIELD(PROPS<I>, "=", 2)
         IF UPCASE(KEY) = "RTHOME"   THEN ALF = FIELD(PROPS<I>, "=", 2)
      NEXT I
      IF AFL = "" THEN CRT PROG:": RTAUDLOG is undefined" ; STOP
      IF ALF = "" THEN CRT PROG:": RTHOME is undefined"; STOP
      *
      READ REGISTER FROM BP.UPL, "register" ELSE REGISTER = ""
      IF TRIM(REGISTER) = "" THEN CRT PROG:": no files are registered"; STOP
      *
      * --------------------------------
      * REGISTER of files to stream     
      * {account} {file}                
      * --------------------------------
      *
      * ----------------------------------------------------- *
      LOGFILE = AFL
      IF LOGFILE = '' THEN 
         LOG.MSG  = "No logfile specified in BP.UPL properties"
         CALL uLOGGER(0, LOG.KEY:LOG.MSG)
         ZERROR = @TRUE
         RETURN
      END
      OPEN LOGFILE TO CLOG ELSE 
         LOG.MSG  = "Open failure: ":LOGFILE
         CALL uLOGGER(0, LOG.KEY:LOG.MSG)
         ZERROR = @TRUE
         RETURN
      END
      RETURN
   END
