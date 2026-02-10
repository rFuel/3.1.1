      SUBROUTINE SR.GETCUSTOMERv2 (ERR, DSD, atID, CORREL, PAYLOAD)
$INCLUDE I_Prologue
      *
      ERR      = ""
      PAYLOAD  = ""
      MARKER   = "<tm>"
      T.MARK   = ""
      F.MARK   = "<fm>"
      LOG.KEY = "CDR-OB":@FM
      IF INF.LOGGING THEN
         LOG.MSG = "Start SR.GETCUSTOMERv2 for ":atID:" ------------------------------"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      ERR = "(GETCUSTOMERv2) Bad or Missing Parameters"
      *
      IF DSD         = ""           THEN GO END..SRTN
      IF atID        = ""           THEN GO END..SRTN
      IF CORREL      = ""           THEN GO END..SRTN
      *
      ERR = ""
      LOWDATE = -999999
      *
      IF INDEX(atID, ":", 1) THEN
         KEY = EREPLACE(atID, ":", @FM)
         LII = KEY<1>           ; * Log In ID
         atID = KEY<2>          ; * Client ID
         * -----------------------------------------------------
         * IF atID = "" THEN get it from the RBI.USER file (LII)
         * -----------------------------------------------------
      END
      *
      DIM CALL.STRINGS(20) ; MAT CALL.STRINGS = ""
      CALL SR.CDR.PAYLOADv2 ( ERR, DSD, atID, LOWDATE, MAT CALL.STRINGS, PAYLOAD )
      *
      * --------------------------------------------------------
END..SRTN:
      IF ERR # "" AND INF.LOGGING THEN
         LOG.MSG = "   .) ERROR: ":ERR
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      IF INF.LOGGING THEN
         LOG.MSG = "Finished extracts on ":atID:" ------------------------------"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
         LOG.MSG = "   ."
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      RETURN
      * --------------------------------------------------------
   END
