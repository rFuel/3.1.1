      SUBROUTINE SR.ORPHANKEYS (ANS, FILE, KEYS)
$INCLUDE I_Prologue
      * --------------------------------------------------------
      * reclaim ALL items in FILE # "ulog"
      * --------------------------------------------------------
      CALL SR.FILE.OPEN(ERR, FILE, IOFILE)
      IF ERR # "" THEN ANS = ERR; RETURN
      CNT      = 0
      DOT      = "."
      ULOG     = ".ulog"
      BASE.DTM = "NOW"
      OPER     = "SUB"
      INTERVAL = "60"
      PERIOD   = "s"                                ;* seconds
      RESULT   = ""
      ERR      = ""
      CALL SR.DTMATH (ERR, BASE.DTM, OPER, INTERVAL, PERIOD, RESULT)
      IF ERR # "" THEN RETURN
      EXPIRE.DT   = FIELD(RESULT, "_", 1)
      EXPIRE.TM   = FIELD(RESULT, "_", 2)
      TODAY       = DATE()
      NOW         = INT(TIME())
      CMD = "SELECT ":FILE:"  UNLIKE ...":ULOG:"..."
      EXECUTE CMD CAPTURING JUNK
      LOOP
         READNEXT ID ELSE EXIT
            IF INDEX(ID, ULOG, 1) THEN CONTINUE
            * is it an orphan? ------------------------------------
            EXPIRE.DT = FIELD(ID, DOT, 7)           ;* Reserver date
            EXPIRE.TM = FIELD(ID, DOT, 8)           ;* Reserver time
            RESET = 0
            IF EXPIRE.DT < TODAY THEN RESET = 1     ;* midnight
            IF EXPIRE.TM < NOW   THEN RESET = 1
            IF NOT(RESET) THEN CONTINUE;
            * -----------------------------------------------------
            nID  = ID[1, INDEX(ID, DOT, 5)-1]:ULOG
            READU REC FROM IOFILE, ID LOCKED
               RELEASE IOFILE, ID
               CONTINUE
            END ELSE
               RELEASE IOFILE, ID
               CONTINUE
            END
            WRITE REC ON IOFILE, nID
            DELETE IOFILE, ID
            CNT+=1
      REPEAT
      ANS := "Reset ":CNT:" orphaned events"
      RETURN
   END
