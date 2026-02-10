      SUBROUTINE SR.GETACCOUNTS (ERR, DSD, atID, CORREL, PAYLOAD)
$INCLUDE I_Prologue
      *
      ERR      = ""
      PAYLOAD  = ""
      MARKER   = "<tm>"
      T.MARK   = ""
      F.MARK   = "<fm>"
      LOG.KEY  = "CDR-OB":@FM
      LII      = ""
      MBR      = ""
      IF INDEX(atID, ":", 1) THEN
         KEY = EREPLACE(atID, ":", @FM)
         LII = KEY<1>
         MBR = KEY<2>
      END
      *
      IF INF.LOGGING THEN
         LOG.MSG = "Start SR.GETACCOUNTS for ":atID:" ------------------------------"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      ERR = "(GETACCOUNTS) Bad or Missing Parameters"
      IF DSD         = "" THEN GO END..SRTN
      IF LII         = "" THEN GO END..SRTN
      IF MBR         = "" THEN GO END..SRTN
      IF CORREL      = "" THEN GO END..SRTN
      *
      CALL SR.FILE.OPEN (ERR, "TRAN"         , TRAN         ) ; IF ERR # "" THEN GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "L_RATES"      , L.RATES      ) ; IF ERR # "" THEN GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "DELINQUENCY"  , DELINQUENCY  ) ; IF ERR # "" THEN GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "CDC.ACCOUNT"  , CDC.ACCOUNT  ) ; IF ERR # "" THEN GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "IC.CONTROL"   , IC.CONTROL   ) ; IF ERR # "" THEN GO END..SRTN
      *
      ERR = ""
      PROC.LIST = ""
      CALL SR.CDR.VERIFY.ACCTS ( ERR, atID, PROC.LIST )
      IF ERR # "" THEN GO END..SRTN
      *
      DIM CALL.STRINGS(20) ; MAT CALL.STRINGS = ""
      IF INF.LOGGING THEN
         LOG.MSG = "   .) building the DSD"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      CALL SR.CDR.DSD ( ERR, DSD, MAT CALL.STRINGS )
$IFDEF isRT
         MATBUILD testARR FROM CALL.STRINGS
         IF DCOUNT(testARR, @FM) = 0 THEN GO END..SRTN
$ELSE
         IF INMAT(CALL.STRINGS) = 0  THEN GO END..SRTN
$ENDIF
      IF INF.LOGGING THEN
         LOG.MSG = "   .) finished."
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      *
      EOA = DCOUNT(PROC.LIST, @VM)
      IF EOA > 0 THEN
         IF INF.LOGGING THEN
            LOG.MSG = "   .) ":MBR:" has ":EOA:" accounts to be processed."
            CALL uLOGGER(1, LOG.KEY:LOG.MSG)
         END
         PROCESSED = ""
         FOR ACT = 1 TO EOA
            RESULT = ""
            II = ACT "R#3"
            atID = PROC.LIST<1, ACT>
            LOCATE(atID, PROCESSED, 1; FND) THEN CONTINUE
            *
            IF INF.LOGGING THEN
               LOG.MSG = "   .) ":II:" loading ":atID
               CALL uLOGGER(1, LOG.KEY:LOG.MSG)
            END
            CALL SR.CDR.PAYLOAD ( ERR, DSD, atID, LOWDATE, MAT CALL.STRINGS, RESULT )
            * --------------------------------------------------
            *        Account Balances                           
            * --------------------------------------------------
            OKAY = 1
            GOSUB CALC..BALANCE
            *
            RESULT := CURRENT:MARKER:AVAILABLE
            ACT.TYPE = FIELD(atID, ".", 1)
            BEGIN CASE
               CASE INDEX(ACT.TYPE, "S", 1)
                  ACT.TYPE = "S":FIELD(ACT.TYPE, "S", 2):"*"
               CASE INDEX(ACT.TYPE, "L", 1)
                  ACT.TYPE = "L":FIELD(ACT.TYPE, "L", 2):"*"
               CASE INDEX(ACT.TYPE, "I", 1)
                  ACT.TYPE = "I":FIELD(ACT.TYPE, "I", 2):"*"
            END CASE
            READV PRODUCT.NAME FROM IC.CONTROL, ACT.TYPE, 1 ELSE PRODUCT.NAME = ACT.TYPE:" unknown"
            RESULT := MARKER:PRODUCT.NAME
            *
            IF INF.LOGGING THEN
               LOG.MSG = "      .) ":RESULT
               CALL uLOGGER(1, LOG.KEY:LOG.MSG)
            END
            IF PAYLOAD # "" THEN PAYLOAD := F.MARK
            PAYLOAD := RESULT
            PROCESSED<1, -1> = atID
         NEXT ACT
      END
      * --------------------------------------------------------
END..SRTN:
      IF ERR # "" AND INF.LOGGING THEN
         LOG.MSG = "   .) ERROR: ":ERR
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      IF INF.LOGGING THEN
         LOG.MSG = "Finished extracts on ":MBR:" ------------------------------"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
         LOG.MSG = "   ."
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      RETURN
      * --------------------------------------------------------
CALC..BALANCE:
      CURRENT     = "0.00"
      AVAILABLE   = "0.00"
      IF (INDEX(atID, "S", 1)) THEN isSAV=1 ELSE isSAV=0
      IF (INDEX(atID, "L", 1)) THEN isLOA=1 ELSE isLOA=0
      IF (INDEX(atID, "I", 1)) THEN isINV=1 ELSE isINV=0
      READ TREC FROM TRAN, atID ELSE
         IF INF.LOGGING THEN
            LOG.MSG = "      .) TRAN missing"
            CALL uLOGGER(1, LOG.KEY:LOG.MSG)
         END
         OKAY = 0
         RETURN
      END
      READ CDC  FROM CDC.ACCOUNT, atID ELSE CDC=""
      CURRENT     = 0
      FREEZE      = 0
      UNCLEARED   = 0
      PENDING     = 0
      CREDIT.LIMIT= 0
      LOAN.ADVANCE= 0
      *
      IF isSAV THEN 
         CURRENT = TREC<1,1,1>+0
         FREEZE.CODE = TREC<6,2,1>
         CREDIT.LIMIT= (TREC<4,5,1> * 100)
         BEGIN CASE
            CASE FREEZE.CODE = 1 OR FREEZE.CODE = 2
               AVAILABLE = 0
               GO END..SUBR
            CASE FREEZE.CODE = 3
               FREEZE        = TREC<6,2,2>
            CASE FREEZE.CODE = 4
               READV FREEZE FROM TRAN, TREC<6,2,2>, 1 ELSE FREEZE = 0
               FREEZE = FREEZE<1,1,1>
            CASE FREEZE.CODE = 5
               READV FZ.AMT FROM TRAN, TREC<6,2,2>, 1 ELSE FZ.AMT = 0
               PCT           = TREC<6,2,3>
               FREEZE        = FZ.AMT<1,1,1> * PCT
               FREEZE        = INT(FREEZE / 100)
         END CASE
         EOI = DCOUNT(TREC<16,2>, @SM)
         FOR P = 1 TO EOI
            UNCLEARED += (TREC<16,2,P>+0)
         NEXT P
         PENDING     = CDC<10,1,1>+0
         AVAILABLE   = (CURRENT - UNCLEARED - PENDING - FREEZE) + CREDIT.LIMIT + LOAN.ADVANCE
      END
      IF isLOA THEN
         CURRENT = TREC<1,1,1>+0
         LTYPE = FIELD(atID[INDEX(atID, "L", 1), 9], ".", 1)
         READ RD.CTL FROM IC.CONTROL, "RD.CONTROL" THEN
            LOCATE(LTYPE, RD.CTL, 3; LPOS) THEN
               READV RATE.REC FROM L.RATES, atID, 1 THEN
                  USE.AV = 0
                  IF RATE.REC<1,3,1> = "V" THEN USE.AV = 19
                  IF RATE.REC<1,3,1> = "F" THEN USE.AV = 20
                  IF RATE.REC<1,3,1> = "C" THEN USE.AV = 21
                  IF RATE.REC<1,3,1> = "C" THEN USE.AV = 22
                  IF USE.AV # 0 THEN                  
                     READ DELINQ FROM DELINQUENCY, atID THEN
                        IF DELINQ<6,1,1> = "" THEN
                           IF DELINQ<5,1,1> => RD.CTL<6,LPOS, 1> THEN 
                              LOAN.ADVANCE = DELINQ<5,1,1>
                           END
                        END
                     END
                  END
               END
            END
         END
         AVAILABLE = LOAN.ADVANCE
      END
      IF isINV THEN
         CURRENT   = TREC<1,1,1>+0
         AVAILABLE = LOAN.ADVANCE
      END
      *
END..SUBR:
      CURRENT     = OCONV(CURRENT, "MD2")
      AVAILABLE   = OCONV(AVAILABLE, "MD2")
      RETURN
      * --------------------------------------------------------
   END
