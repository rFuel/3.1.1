      SUBROUTINE SR.OPEN.CREATE (ERR, INFILE, TYPE, HANDLE)
$INCLUDE I_Prologue
      ERR = 0
      PRECISION 9
      STX = TIME()
      IF TYPE = "" THEN TYPE = "DYNAMIC"
      LOG.KEY = MEMORY.VARS(1):@FM
      CALL SR.FILE.OPEN(ERR, INFILE, HANDLE)
      IF ERR # "" THEN 
         IF DBT = "UV" THEN
            * Create CHK2 (raw file name) but open INFILE
            CALL SR.GET.PROPERTY("dacct", DACCT)
            CHK1 = EREPLACE(INFILE, "_":DACCT, "")
            CHK2 = EREPLACE(CHK1, "upl_", "")
            EXECUTE "CREATE-FILE ":CHK2:" ":TYPE CAPTURING JUNK
            MSG = "CREATE-FILE ":CHK2:" ":TYPE
            IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:MSG)
            CALL SR.FILE.OPEN(ERR, INFILE, HANDLE)
            IF ERR # "" AND TYPE = "DYNAMIC" THEN
               EXECUTE  "CREATE-FILE ":CHK2:" 30 3 4" CAPTURING JUNK
               MSG = "CREATE-FILE ":CHK2:"  30 3 4"
               IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:MSG)
               CALL SR.FILE.OPEN(ERR, INFILE, HANDLE)
               IF ERR # "" THEN
                  MSG = "ABORT - Cannot CREATE-FILE ":INFILE:" ":ERR
                  IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:MSG)
               END
            END
         END
         IF DBT = "UD" THEN
            IF TYPE = 1 OR TYPE = 19 THEN TYPE = "DIR"
            EXECUTE "CREATE.FILE ":TYPE:" ":INFILE CAPTURING JUNK
            MSG = "    CREATE.FILE ":TYPE:" ":INFILE
            IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:MSG)
            CALL SR.FILE.OPEN(ERR, INFILE, HANDLE)
            IF ERR # "" THEN
               MSG = "ABORT - Cannot CREATE.FILE ":TYPE:" ":INFILE
               IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:MSG)
            END
         END
         IF DBT = "RT" THEN
            IF TYPE = 1 OR TYPE = 19 THEN 
***            EXE = "DIR-VIEW ":INFILE:" . "
               EXE = "CREATE-FILE ":INFILE:" 1,1 17,1 ALU"
            END ELSE
               EXE = "CREATE-FILE ":INFILE:" 1,1 7,1 ALU"
            END
            EXECUTE EXE CAPTURING JUNK
            MSG = "    ":EXE
            IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:MSG)
            CALL SR.FILE.OPEN(ERR, INFILE, HANDLE)
            IF ERR # "" THEN
               MSG = "ABORT - Cannot CREATE.FILE ":TYPE:" ":INFILE
               IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:MSG)
            END
         END
      END
END..PROGRAM:
      
      RETURN
   END
