      SUBROUTINE SR.CDR.VERIFY01 ( OKAY, inID )
$INCLUDE I_Prologue
      LOG.KEY = "CDR-OB":@FM
      *
      OKAY = 0
      LII  = UPCASE(FIELD(inID, ":", 1))
      atID = FIELD(inID, ":", 2)
      *
      CALL SR.FILE.OPEN (ERR, "RBI.USER"  , RBI.USER ) ; IF ERR # "" THEN GO END..SRTN
      READ RBI.RECORD FROM RBI.USER, LII ELSE RBI.RECORD = ""
      USER.ACL    = RBI.RECORD<16>
      IF USER.ACL = 0 THEN OKAY = 1
      IF USER.ACL = 1 THEN OKAY = 1
      IF NOT(OKAY) THEN 
         IF INF.LOGGING THEN
            LOG.MSG = "   .) ERROR: Invalid User Access Level [":LII:"]"
            CALL uLOGGER(1, LOG.KEY:LOG.MSG)
         END
      END
      PRIMARY.CLID= RBI.RECORD<1>
      SECONDARY.ID= RBI.RECORD<4>
      IF atID = "" THEN atID = PRIMARY.CLID
      OKAY = 0
      IF atID = PRIMARY.CLID THEN 
         OKAY = 1
      END ELSE
         IF SECONDARY.ID # "" AND atID = SECONDARY.ID THEN OKAY = 1
      END
      IF NOT(OKAY) THEN 
         IF INF.LOGGING THEN
            LOG.MSG = "   .) ERROR: Primary client ID mis-match."
            CALL uLOGGER(1, LOG.KEY:LOG.MSG)
         END
         OKAY = 0
      END
      IF OKAY THEN inID = LII:":":atID
END..SRTN:
      RETURN
   END
