      SUBROUTINE SR.CDR.VERIFY.ACCTS ( ERR, atID, PROC.LIST )
$INCLUDE I_Prologue
      LOG.KEY     = "CDR-OB":@FM
      ERR         = ""
      ACC.INDEX   = ""
      EXCL.LIST   = ""
      PROC.LIST   = ""
      LII         = ""
      *
      IF INDEX(atID, ":", 1) THEN LII = FIELD(atID, ":", 1)
      IF LII # "" THEN
         OKAY = 0
         CALL SR.CDR.VERIFY01 ( OKAY, atID )
         IF NOT(OKAY) THEN 
            ERR = "Failed User Access Level verification."
            GO END..SRTN
         END
         atID = FIELD(atID, ":", 2)
      END ELSE
         LII = atID
      END
      *
      CALL SR.FILE.OPEN (ERR, "CLIENT"    , CUSTOMER ) ; IF ERR # "" THEN GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "ACCOUNT"   , ACCOUNT  ) ; IF ERR # "" THEN GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "RBI.USER"  , RBI.USER ) ; IF ERR # "" THEN GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "BP.UPL"    , BP.UPL   ) ; IF ERR # "" THEN GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "TRAN.EXT"  , TRANEXT  ) ; IF ERR # "" THEN GO END..SRTN
      *
      READ ACT.TYPES FROM BP.UPL, "CDR-ACCOUNT-TYPES" ELSE ACT.TYPES = ""
      READ RBI.RECORD FROM RBI.USER, LII ELSE 
         READ RBI.RECORD FROM RBI.USER, UPCASE(LII) ELSE RBI.RECORD = ""
      END
      * ## 001
      PRI.ID = RBI.RECORD<1,1,1>
      SEC.ID = RBI.RECORD<4,1,1>
      EOM = DCOUNT(RBI.RECORD<6>, @VM)
      FOR I = 1 TO EOM
         IF RBI.RECORD<7, I>  > 1 THEN
            EXCL.LIST<1,-1> = RBI.RECORD<6,I>      ; * Account Acl is NOT okay
         END
         ACC.INDEX<1,-1> = RBI.RECORD<6,I>         ; * Account Acl is okay
      NEXT M
      *
      READ CLI.RECORD FROM CUSTOMER, atID ELSE 
         ERR = "This client does not exist."
         CALL uLOGGER(1, LOG.KEY:ERR :" [":atID:"]")
         GO END..SRTN
      END
      LOWDATE = -999999
      CLID = atID
      ACC.LIST  = CLI.RECORD<40>
      ASSOC.IDX = CLI.RECORD<57>
      EOM = DCOUNT(ACC.LIST, @VM)
      FOR I = 1 TO EOM
         ACC.INDEX<1,-1> = atID:ACC.LIST<1,I>
      NEXT I
      ACC.INDEX := @VM : ASSOC.IDX : @VM : ASSOC.IDX
      *
      EOA = DCOUNT(ACC.INDEX, @VM)
      FOR ACT = 1 TO EOA
         atID = ACC.INDEX<1, ACT>
         LOCATE(atID, EXCL.LIST, 1; FND) THEN 
            IF INF.LOGGING THEN
               LOG.MSG = "   ** excluding ":atID:" - no digital access on account"
               CALL uLOGGER(1, LOG.KEY:LOG.MSG)
            END
            CONTINUE
         END
         READ ACC.RECORD FROM ACCOUNT, atID ELSE CONTINUE
         IF ACC.RECORD<4,1,1> # "" THEN
            *
            * ----------- CLOSED date is not empty ----------------
            *
            trID = atID:"/0"
            READ RECORD FROM TRANEXT, trID ELSE
               *
               * ---------- trans are archived: ignore this -------
               *
               IF INF.LOGGING THEN
                  LOG.MSG = "   ** excluding ":trID:" - account is CLOSED"
                  CALL uLOGGER(1, LOG.KEY:LOG.MSG)
               END
               CONTINUE
            END
         END
         OKAY = 0
         LOCATE(PRI.ID, ACC.RECORD, 29; FND) THEN OKAY = 1
         IF SEC.ID # "" AND OKAY THEN
            LOCATE(SEC.ID, ACC.RECORD, 29; FND) THEN OKAY = 1 ELSE OKAY = 0
         END
         IF OKAY THEN
            ACTYPE = ACC.RECORD<30, FND>
            INCL = 0
            IF ACTYPE # "" THEN
               IF INDEX(ACT.TYPES, ACTYPE, 1) THEN INCL = 1
            END ELSE
               INCL = 1
            END
            *
            IF INCL THEN 
               PROC.LIST<1, -1> = atID
            END ELSE
               IF INF.LOGGING THEN
                  LOG.MSG = "   ** excluding ":atID:" - act type (":ACTYPE:") exclusion"
                  CALL uLOGGER(1, LOG.KEY:LOG.MSG)
               END
            END
**         END ELSE
**            PROC.LIST<1, -1> = atID
         END
      NEXT ACT
END..SRTN:
      IF ERR # "" AND INF.LOGGING THEN
         LOG.MSG = "   .) ERROR: ":ERR
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      RETURN
   END
