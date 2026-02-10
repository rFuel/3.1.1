      SUBROUTINE SR.GETCUSTOMER (ERR, DSD, atID, CORREL, PAYLOAD)
$INCLUDE I_Prologue
      *
      ERR      = ""
      PAYLOAD  = ""
      MARKER   = "<tm>"
      T.MARK   = ""
      F.MARK   = "<fm>"
      LOG.KEY  = "CDR-OB":@FM
      LII      = ""
      *
      IF INDEX(atID, ":", 1) THEN
         KEY = EREPLACE(atID, ":", @FM)
         LII = KEY<1>
      END
      ERR = "(GETCUSTOMER) Bad or Missing Parameters"
      *
      IF DSD         = ""           THEN GO END..SRTN
      IF CORREL      = ""           THEN GO END..SRTN
      ERR = "Bad structure of customer, use loginID:clientID."
      IF LII         = ""           THEN GO END..SRTN
      IF atID        = ""           THEN GO END..SRTN
      *
      IF INF.LOGGING THEN
         LOG.MSG = "Start SR.GETCUSTOMER for ":atID:" ------------------------------"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      OKAY = 0
      CALL SR.CDR.VERIFY01 ( OKAY, atID )
      IF NOT(OKAY) THEN 
         ERR = "Failed initial verification."
         GO END..SRTN
      END
      atID = FIELD(atID, ":", 2)  
      ERR = ""
      LOWDATE = -999999
      *
      DIM CALL.STRINGS(20) ; MAT CALL.STRINGS = ""
      CALL SR.CDR.PAYLOAD ( ERR, DSD, atID, LOWDATE, MAT CALL.STRINGS, PAYLOAD )
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
