      * External Interface Manager - ALWAYS running
$INCLUDE I_Prologue
      *
      MEMORY.VARS(1) = "uADMIN"
      LOG.KEY = MEMORY.VARS(1):@FM
      ERR = ""
      CALL SR.FILE.OPEN(ERR, "uREQUESTS",    uREQUESTS)
      CALL SR.FILE.OPEN(ERR, "sRESPONSES",   sRESPONSES)
      CALL SR.FILE.OPEN(ERR, "RETURN.CODES", RETURN.CODES)
      IF ERR # "" THEN STOP
      *
      EXTN = "admin"
      LOOP
         SELECT uREQUESTS
         LOOP
            READNEXT ID ELSE EXIT
            POSX = DCOUNT(ID, ".")
            THIS = FIELD(ID, ".", POSX)
            IF THIS # EXTN THEN CONTINUE
            READU REQUEST FROM uREQUESTS, ID ELSE CONTINUE
            DELETE uREQUESTS, ID
            GOSUB HANDLE..ADMIN
            RELEASE uREQUESTS, ID
         REPEAT
         RQM; RQM
      REPEAT
      * -----------------------------------------------------
HANDLE..ADMIN:
      CALL SR.METABASIC(REPLY, REQUEST)
      MSG = ID:" Sent to sRESPONSES"
      IF INF.LOGGING THEN CALL uLOGGER(5, LOG.KEY:MSG)
      WRITE REPLY ON sRESPONSES, ID
      RETURN
   END
