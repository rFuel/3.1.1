      SUBROUTINE SR.GETACCOUNTEXTRA (ERR, atID, PAYLOAD )
$INCLUDE I_Prologue
      *
      ERR      = ""
      PAYLOAD  = ""
      MARKER   = "<tm>"
      T.MARK   = ""
      F.MARK   = "<fm>"
      LOG.KEY  = "CDR-OB":@FM
      *
      IF INF.LOGGING THEN
         LOG.MSG = "Start SR.GETACCOUNTEXTRA for ":atID:" ------------------------------"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      *
      CALL SR.FILE.OPEN (ERR, "CLIENT" , CLIENT ) ; IF ERR # "" THEN GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "ACCOUNT", ACCOUNT) ; IF ERR # "" THEN GO END..SRTN
      *
      READ ACCOUNT.REC FROM ACCOUNT, atID ELSE ACCOUNT.REC = ""
      REL.TYPE = ""
      FIRSTNAME= ""
      LASTNAME = ""
      EOM = DCOUNT(ACCOUNT.REC<29>, @VM)
      FOR M = 1 TO EOM
         MBR = ACCOUNT.REC<29, M, 1>
         READ CLIENT.REC  FROM CLIENT,  MBR  ELSE CLIENT.REC = ""
         REL.TYPE = ACCOUNT.REC<30, M, 1>
         FIRSTNAME= CLIENT.REC<7,1,1>
         LASTNAME = CLIENT.REC<1,1,1>
         *
         PAYLOAD<1, -1> = MBR
         PAYLOAD<2, -1> = FIRSTNAME
         PAYLOAD<3, -1> = LASTNAME
         PAYLOAD<4, -1> = REL.TYPE
      NEXT M
END..SRTN:
      IF INF.LOGGING THEN
         LOG.MSG = "Finished extracts on ":atID:" ------------------------------"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
         LOG.MSG = "   ."
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      RETURN
   END
