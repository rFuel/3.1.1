      SUBROUTINE SR.GETTRANS (ERR, PG, SZ, CORREL, PAYLOAD)
$INCLUDE I_Prologue
      *
      ERR      = ""
      PAYLOAD  = ""
      SLISTS   = "&SAVEDLISTS&"
      F.MARK   = "<fm>"
      LOG.KEY = "CDR-OB":@FM
      *
      IF CORREL      = ""           THEN GO END..SRTN
      IF PG = "" OR NOT(NUM(PG))    THEN GO END..SRTN
      IF SZ = "" OR NOT(NUM(SZ))    THEN GO END..SRTN
      CALL SR.FILE.OPEN (ERR, SLISTS    , SL     ) ; IF ERR # "" THEN GO END..SRTN
      READ RECORD FROM SL, CORREL ELSE
         ERR = CORREL : " cannot be found in " : SLISTS
         GO END..SRTN
      END
      *
      * ------------------------------------------------------------------
      * Move to the starting line for this page {PG}
      * ------------------------------------------------------------------
      START.FROM = (PG * SZ) - SZ
      *
      FOR I = 1 TO START.FROM
         REMOVE LINE FROM RECORD SETTING JUNK
      NEXT I
      * ------------------------------------------------------------------
      * Now read a page full of lines into PAYLOAD and return it
      * ------------------------------------------------------------------
      FINISH.AT   = START.FROM + SZ
      START.FROM += 1
      FOR I = START.FROM TO FINISH.AT
         REMOVE LINE FROM RECORD SETTING JUNK
         PAYLOAD := LINE : F.MARK
         IF JUNK = 0 THEN EXIT
      NEXT I
END..SRTN:
      IF ERR # "" AND INF.LOGGING THEN
         LOG.MSG = "   .) ERROR: ":ERR
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      IF INF.LOGGING THEN
         LOG.MSG = "Finished extracts     ----------------------------------------"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
         LOG.MSG = "   ."
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      RETURN
   END
