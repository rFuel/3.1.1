      SUBROUTINE SR.GETPAYMENTS (ERR, DSD, atID, CORREL, PAYLOAD)
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
      ERR = "(GETPAYMENTS) Bad or Missing Parameters"
      IF DSD         = "" THEN GO END..SRTN
      IF atID        = "" THEN GO END..SRTN
      IF CORREL      = "" THEN GO END..SRTN
      *
      IF INDEX(atID, ":", 1) THEN
         KEY = EREPLACE(atID, ":", @FM)
         LII = KEY<1>
         atID= KEY<2>
      END
      ERR = "Bad structure of customer, use loginID:clientID."
      IF LII         = "" THEN GO END..SRTN
      IF atID        = "" THEN GO END..SRTN
      ERR = ""
      *
      IF INF.LOGGING THEN
         LOG.MSG = "Start SR.GETPAYMENTS for ":atID:" ------------------------------"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      ERR = ""
      PROC.LIST = ""
      CALL SR.CDR.VERIFY.ACCTS ( ERR, LII:":":atID, PROC.LIST )
      IF ERR # "" THEN GO END..SRTN
      *
      CALL SR.FILE.OPEN (ERR, "BP.UPL"       , BP.UPL        ) ; IF ERR # "" THEN GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "ACCOUNT"      , ACCOUNT       ) ; IF ERR # "" THEN GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "TRAN"         , TRAN          ) ; IF ERR # "" THEN GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "AFFP"         , AFFP          ) ; IF ERR # "" THEN GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "DES.DDA"      , DES.DDA       ) ; IF ERR # "" THEN GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "CLIENT"       , CUSTOMER      ) ; IF ERR # "" THEN GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "DES.REMITTER" , DES.REMITTER  ) ; IF ERR # "" THEN GO END..SRTN
      *
      * Get the BSB
      *
      READ BSB FROM BP.UPL, "BSB" ELSE BSB="not-provided"
      *
      * Make sure the primary customer exists !!
      *
      READ RECORD FROM CUSTOMER, atID ELSE
         ERR = atID:" was not found in the customer file."
         GO END..SRTN
      END
      *
      * Now get all associated customer records and their DD's and payments.
      *
      CUST.LIST = atID
      EOC = DCOUNT(PROC.LIST, @VM)
      FOR I = 1 TO EOC
         ACID = PROC.LIST<1, I>
         CLID = FIELD(ACID, "S", 1)
         CLID = FIELD(CLID, "L", 1)
         CLID = FIELD(CLID, "I", 1)
         LOCATE(CLID, CUST.LIST, 1; FND) ELSE
            IF INF.LOGGING THEN
               LOG.MSG = "   .) associating client [":CLID:"]"
               CALL uLOGGER(1, LOG.KEY:LOG.MSG)
            END
            CUST.LIST<1,-1> = CLID
         END
      NEXT I
      *
      EOC = DCOUNT(CUST.LIST, @VM)
      *
      * -------------------- [ Direct Debits ] ---------------------------------------------
      *
      IF INF.LOGGING THEN
         LOG.MSG = "   .) collecting Direct Debits."
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      FOR X = 1 TO EOC
         CLID = CUST.LIST<1,X>
         READ RECORD FROM CUSTOMER, CLID ELSE
            ERR = atID:" was not found in the customer file."
            GO END..SRTN
         END 
         DDEBITS  = RECORD<43>
         GOSUB DO..DDEBITS
      NEXT X
      *
      * -------------------- [ Future Payments ] -------------------------------------------
      *
      IF INF.LOGGING THEN
         LOG.MSG = "   .) collecting Future Payments."
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      FOR X = 1 TO EOC
         CLID = CUST.LIST<1,X>
         READ RECORD FROM CUSTOMER, CLID ELSE
            ERR = atID:" was not found in the customer file."
            GO END..SRTN
         END 
         PAYMENTS = RECORD<26>
         GOSUB DO..PAYMENTS
      NEXT X
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
DO..DDEBITS:
      EOP = DCOUNT(DDEBITS, @VM)
      FOR I = 1 TO EOP
         RECID = DDEBITS<1,I>
         READ DDA.REC FROM DES.DDA, RECID ELSE
            IF INF.LOGGING THEN
               LOG.MSG = "   .) DD [":RECID:"] cannot be found. Excluding it."
               CALL uLOGGER(1, LOG.KEY:LOG.MSG)
            END
            CONTINUE
         END
         DRID = RECID[INDEX(RECID, "*", 1)+1, LEN(RECID)]
         READ DDR.REC FROM DES.REMITTER, DRID ELSE
            IF INF.LOGGING THEN
               LOG.MSG = "   .) DD Remitter [":DRID:"] cannot be found. Excluding it."
               CALL uLOGGER(1, LOG.KEY:LOG.MSG)
            END
            CONTINUE
         END
         LINE = ""
         GOSUB CLEAR..VARIABLES
         PMTTYP = "directDebit"
         *
         IF INF.LOGGING THEN
            LOG.MSG = "      .) loading DD [":RECID:"] last debited from ":DDA.REC<6,1,1>
            CALL uLOGGER(1, LOG.KEY:LOG.MSG)
         END
         DESC   = DDR.REC<1,1,1>
         LDAMT  = OCONV(DDA.REC<7,1,1>, "MD2")
         LDDTE  = OCONV(DDA.REC<5,1,1>, "D4-")
         DDSTAT = DDA.REC<3,1,1>
         DDACCT = DDA.REC<6,1,1>
         *
         GOSUB BUILD..OUTLINE
         *
         IF PAYLOAD # "" THEN PAYLOAD := F.MARK
         PAYLOAD := LINE
      NEXT I
      RETURN
DO..PAYMENTS:
      EOP = DCOUNT(PAYMENTS, @VM)
      FOR I = 1 TO EOP
         RECID = "F":PAYMENTS<1,I>
         READ PAY.REC FROM AFFP, RECID ELSE
            IF INF.LOGGING THEN
               LOG.MSG = "   .) [":RECID:"] cannot be found. Excluding it."
               CALL uLOGGER(1, LOG.KEY:LOG.MSG)
            END
            CONTINUE
         END
         LINE = ""
         GOSUB CLEAR..VARIABLES
         MBR.ID= PAY.REC<5,1>
         ACT.CD= PAY.REC<5,2>
         ACCTID= MBR.ID:ACT.CD
         METHOD= PAY.REC<3>
         PMTTYP = "scheduledPayment"
         *
         SHOW.ACCT = ""
         AMOUNT = OCONV(PAY.REC<2,1,1>, "MD2")
         IF NUM(AMOUNT) THEN CALCD = "false" ELSE CALCD = "true"; AMOUNT = ""
         SPACT  = ACCTID
         BEGIN CASE
            CASE METHOD = 0
               NICK   = PAY.REC<19,1,1>
               PEEREF = PAY.REC<20,1,1>
               PERREF = PAY.REC<26,1,1>
               TO.ACCT = PAY.REC<8,1,1>:PAY.REC<8,2,1>
               SHOW.ACCT = TO.ACCT
               * -----------------------
               READV DACCT FROM TRAN   , TO.ACCT, 4   ELSE DACCT = ""
               READV DNAME FROM ACCOUNT, TO.ACCT, 28  ELSE DNAME = ""
               DACCT = DACCT<1,8,1>
               DNAME = DNAME<1,2,1>
               DBSB  = BSB
               DUTYP = "account"
               * -----------------------
               LOCATE(TO.ACCT, PROC.LIST, 1; FND) THEN
                  TOUTYP= "accountId"
                  ACCT  = TO.ACCT
                  * --------------------------
                  SHOW.ACCT := " - ":ACCTID
                  * --------------------------
               END ELSE
                  TOUTYP= "domestic"
                  ACCT  = ""
                  * --------------------------------------
                  * What about card or payId  DUTYPs    ??
                  * --------------------------------------
               END
            CASE METHOD = 1
               TOUTYP= "DO-NOT-USE-TYPE-1"
               CONTINUE
               ******* not in use *******
            CASE METHOD = 2
               TOUTYP= "DO-NOT-USE-TYPE-2"
               CONTINUE
               ******* not in use *******
            CASE METHOD = 3
               TOUTYP= "DO-NOT-USE-TYPE-3"
               CONTINUE
               ******* not in use *******
            CASE METHOD = 4
               TOUTYP = "domestic"
               * --------------------- [ domestic account ] -------------
               DUTYP = "account"
               DACCT = PAY.REC<8,1,1>
               DNAME = PAY.REC<19,1,1>
               DBSB  = PAY.REC<18,1,1>
               SHOW.ACCT = ACCTID
            CASE METHOD = 5
               TOUTYP= "DO-NOT-USE-TYPE-5"
               CONTINUE
               ******* not in use *******
            CASE METHOD = 6
               TOUTYP = "biller"
               BPAYCD = PAY.REC<8,1,1>
               BPCRN  = PAY.REC<8,2,1>
               BPAYNM = PAY.REC<8,3,1>
               SHOW.ACCT = ACCTID
            CASE METHOD = 7
               TOUTYP= "DO-NOT-USE-TYPE-7"
               CONTINUE
               ******* not in use *******
            CASE METHOD = 8
               TOUTYP= "DO-NOT-USE-TYPE-8"
               CONTINUE
               ******* not in use *******
            CASE METHOD = 9
               TOUTYP = "domestic"
               IF PAY.REC<8>:PAY.REC<18>:PAY.REC<19> # "" THEN
                  * --------------------- [ domestic account ] -------------
                  DUTYP = "account"
                  DACCT = PAY.REC<8,1,1>
                  DNAME = PAY.REC<19,1,1>
                  DBSB  = PAY.REC<18,1,1>
                  SHOW.ACCT = DACCT
               END ELSE
                  * --------------------- [ domestic payId ] -------------
                  DUTYP = "payId"
                  NPPID = PAY.REC<40,1,1>
                  NPPNM = PAY.REC<41,1,1>
                  NPPTY = PAY.REC<39,1,1>
                  SHOW.ACCT = "NPP - payId"
               END
         END CASE
         *
         IF INF.LOGGING THEN
            LOG.MSG = "      .) loading [":RECID:"] from ":SHOW.ACCT
            CALL uLOGGER(1, LOG.KEY:LOG.MSG)
         END
         *
         PAYFREQ = PAY.REC<4,1,1>
         ROCUTYP = "intervalSchedule"
         ROCIFPD = OCONV(PAY.REC<1,2,1>, "D4-")
         ROCNPDT = OCONV(PAY.REC<12,1,1>, "D4-")
         IF PAYFREQ = "1" THEN
            IF PAY.REC<1,1,1> = PAY.REC<1,2,1> THEN
               ROCUTYP= "onceOff"
               ROCOFPD= OCONV(PAY.REC<1,1,1>, "D4-")
            END
         END
         ROCIVAL = PAYFREQ
         *
***      BEGIN CASE
***            CASE PAYFREQ="1"
***               ROCIVAL= "weekly"
***               IF PAY.REC<1,1,1> = PAY.REC<1,2,1> THEN
***                  ROCUTYP= "onceOff"
***                  ROCOFPD= OCONV(PAY.REC<1,1,1>, "D4-")
***               END   
***            CASE PAYFREQ="2"
***               ROCIVAL= "fortnightly"
***               ROCIVAL= PAYFREQ
***            CASE PAYFREQ="3"
***               ROCIVAL= "monthly"
***               ROCIVAL= PAYFREQ
***            CASE PAYFREQ="4"
***               ROCIVAL= "quarterly"
***               ROCIVAL= PAYFREQ
***            CASE PAYFREQ="5"
***               ROCIVAL= "half-yearly"
***               ROCIVAL= PAYFREQ
***            CASE PAYFREQ="6"
***               ROCIVAL= "annually"
***               ROCIVAL= PAYFREQ
***            CASE PAYFREQ="7"
***               ROCIVAL= "4-weekly"
***               ROCIVAL= PAYFREQ
***            CASE PAYFREQ="8"
***               ROCIVAL= "bi-monthly"
***               ROCIVAL= PAYFREQ
***            CASE PAYFREQ="9"
***               ROCIVAL= ""
***               ROCUTYP= ""
***               ROCIFPD= ""
***      END CASE
         ROCSTAT = "ACTIVE"
         *
         GOSUB BUILD..OUTLINE
         *
         IF PAYLOAD # "" THEN PAYLOAD := F.MARK
         PAYLOAD := LINE
      NEXT I
      RETURN
      * --------------------------------------------------------
BUILD..OUTLINE:
      LINE := RECID
      LINE := MARKER:ABN
      LINE := MARKER:ACN
      LINE := MARKER:ARBN
      LINE := MARKER:PMTTYP
      LINE := MARKER:DESC
      LINE := MARKER:DDFI  
      LINE := MARKER:LDAMT 
      LINE := MARKER:DDACCT
      LINE := MARKER:LDDTE 
      LINE := MARKER:DDSTAT
      LINE := MARKER:SPACT 
      LINE := MARKER:NICK  
      LINE := MARKER:PEEREF
      LINE := MARKER:PERREF
      LINE := MARKER:AMOUNT
      LINE := MARKER:CURRCY
      LINE := MARKER:CALCD 
      LINE := MARKER:TOUTYP
      LINE := MARKER:ACCT  
      LINE := MARKER:BPAYCD
      LINE := MARKER:BPAYNM
      LINE := MARKER:BPCRN 
      LINE := MARKER:DUTYP
      LINE := MARKER:DACCT 
      LINE := MARKER:DNAME 
      LINE := MARKER:DBSB  
      LINE := MARKER:CARD  
      LINE := MARKER:NPPID 
      LINE := MARKER:NPPNM 
      LINE := MARKER:NPPTY 
      LINE := MARKER:INTACT
      LINE := MARKER:INTADR
      LINE := MARKER:INTNAM
      LINE := MARKER:INTBIC
      LINE := MARKER:INTCHP 
      LINE := MARKER:INTCONT
      LINE := MARKER:INTWIRE
      LINE := MARKER:INTLEID
      LINE := MARKER:INTROUT
      LINE := MARKER:INTSORT
      LINE := MARKER:INTBCTY
      LINE := MARKER:INTBMSG
      LINE := MARKER:INTBNAM
      LINE := MARKER:PAYEEID
      LINE := MARKER:ROCUTYP
      LINE := MARKER:ROCOFPD
      LINE := MARKER:ROCNPDT
      LINE := MARKER:ROCDESC
      LINE := MARKER:ROCSTAT
      LINE := MARKER:ROCIFPD
      LINE := MARKER:ROCIDAY
      LINE := MARKER:ROCIVAL
      LINE := MARKER:ROCNBDT
      LINE := MARKER:ROCIREM
      LINE := MARKER:LWDFPD 
      LINE := MARKER:LWDINT 
      LINE := MARKER:LWDLWD 
      LINE := MARKER:LWDNBDT
      LINE := MARKER:LWDREM 
      RETURN
      *
CLEAR..VARIABLES:
      * ----------- [ Commons ] ------------------
      ABN   = ""
      ACN   = ""
      ARBN  = ""
      PMTTYP= ""
      * ----------- [ direct debits ] ------------------
      DESC  = ""
      DDFI  = ""
      LDAMT = ""
      DDACCT= ""
      LDDTE = ""
      DDSTAT= ""
      * ----------- [ scheduled pays ] ------------------
      SPACT = ""
      NICK  = ""
      PEEREF= ""
      PERREF= ""
      * ---------------- [ payment set ] ------------------
      AMOUNT= ""
      CURRCY= "AUD"
      CALCD = ""
      TOUTYP= ""
      ACCT  = ""
      * ---------------- [ biller (BPAY) ] ------------------
      BPAYCD= ""
      BPAYNM= ""
      BPCRN = ""
      * ---------------- [ domestic ] ------------------
      DUTYP = ""
      * --------------------- [ domestic account ] -------------
      DACCT = ""
      DNAME = ""
      DBSB  = ""
      * --------------------- [ domestic card ] -------------
      CARD  = ""
      * --------------------- [ domestic payId ] -------------
      NPPNM = ""
      NPPTY = ""
      NPPID = ""
      * ---------------- [ International ] ------------------
      INTACT= ""
      INTADR= ""
      INTNAM= ""
      INTBIC= ""
      INTCHP = ""
      INTCONT= ""
      INTWIRE= ""
      INTLEID= ""
      INTROUT= ""
      INTSORT= ""
      * ---------------- [ International Beneficiary ] ------------------
      INTBCTY= ""
      INTBMSG= ""
      INTBNAM= ""
      * ---------------- [ payeeID ] ------------------
      PAYEEID= ""
      * ----------- [ recurrence block ] ------------------
      ROCUTYP= ""
      ROCDESC= ""
      ROCSTAT= ""
      ROCOFPD= ""
      ROCNPDT= ""
      * ---------------- [ interval ] ------------------
      ROCIFPD= ""
      ROCIDAY= ""
      ROCIVAL= ""
      ROCNBDT= ""
      ROCIREM= ""
      * ---------------- [ last week day ] ------------------
      LWDFPD = ""
      LWDINT = ""
      LWDLWD = ""
      LWDNBDT= ""
      LWDREM = ""
      RETURN
   END
