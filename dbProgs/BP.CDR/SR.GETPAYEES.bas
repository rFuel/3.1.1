      SUBROUTINE SR.GETPAYEES (ERR, DSD, atID, CORREL, PAYLOAD)
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
         LOG.MSG = "Start SR.GETPAYEES for ":atID:" ------------------------------"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      *
      ERR = ""
      PROC.LIST = ""
      useID = atID
      CALL SR.CDR.VERIFY.ACCTS ( ERR, useID, PROC.LIST )
      IF ERR # "" THEN GO END..SRTN
      IF PROC.LIST = "" THEN 
         ERR = "(GETPAYEES) No valid accounts with Payee details."
         GO END..SRTN
      END
      *
      LII      = ""
      IF INDEX(atID, ":", 1) THEN
         KEY = EREPLACE(atID, ":", @FM)
         LII = KEY<1>
         atID= KEY<2>
      END
      ERR = "(GETPAYEES) Bad or Missing Parameters"
      IF LII         = ""           THEN GO END..SRTN
      IF atID        = ""           THEN GO END..SRTN
      IF CORREL      = ""           THEN GO END..SRTN
      *
      ERR = ""
      CALL SR.FILE.OPEN (ERR, "RBI.PAYEE.INDEX"  , RBI.PAYEE.INDEX  ) ; IF ERR # "" THEN GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "RBI.PAYEE"        , RBI.PAYEE        ) ; IF ERR # "" THEN GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "BPAY.BILLER.NAME" , BPAY.BILLER.NAME ) ; IF ERR # "" THEN GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "INTERNATIONAL.PAYEE.INDEX"  , INTERNATIONAL.PAYEE.INDEX ) ; IF ERR # "" THEN GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "INTERNATIONAL.PAYEE"        , INTERNATIONAL.PAYEE       ) ; IF ERR # "" THEN GO END..SRTN
      *
      READ DOMESTIC.INDEX FROM RBI.PAYEE.INDEX, atID ELSE DOMESTIC.INDEX = ""
      READ FOREIGN.INDEX  FROM INTERNATIONAL.PAYEE.INDEX, atID ELSE FOREIGN.INDEX = ""
      *
      EOD = DCOUNT(DOMESTIC.INDEX, @FM)
      FOR D = 1 TO EOD
         *
         PAYEE.ID = DOMESTIC.INDEX<D>
         READ PAYEE FROM RBI.PAYEE, PAYEE.ID THEN
            IF INF.LOGGING THEN
               LOG.MSG = "   .) loading (D) ":PAYEE.ID
               CALL uLOGGER(1, LOG.KEY:LOG.MSG)
            END
            NICKNAME    = ""
            PAYEETYPE   = ""
            BILLERCODE  = ""
            BILLERNAME  = ""
            CRN         = ""
            UTYPE       = ""
            ACCOUNTNAME = ""
            ACCOUNTNBR  = ""
            BSB         = ""
            CARD        = ""
            PAYID       = ""
            PAYNAME     = ""
            PAYTYPE     = ""
            IPAY.ID    = ""
            INTNICKNAM = ""
            INTACCTNBR = ""
            INTBANKADR1= ""
            INTBANKADR2= ""
            INTBANKADR3= ""
            INTBANKADR4= ""
            INTBANKNAM = ""
            INTBANKCDE = ""
            INTBANKBIC = ""
            INTCHIPNBR = ""
            INTCOUNTRY = ""
            INTWIRENBR = ""
            INTENTITYID= ""
            INTROUTENBR= ""
            INTSORTCDE = ""
            INTBENECTY = ""
            INTBENEMSG = ""
            INTBENENAM = ""
            INTDESCRIP = ""
            *
            TYPE = PAYEE<1>
            NICKNAME    = PAYEE<3,1,1>
            BEGIN CASE
               CASE TYPE = 1
                  PAYEETYPE   = "bpay"
                  BILLERCODE  = PAYEE<5,1,1>
                  READV BILLERNAME FROM BPAY.BILLER.NAME, BILLERCODE, 2 ELSE BILLERNAME = "unknown"
                  CRN         = PAYEE<6,1,1>
               CASE TYPE = 2
                  PAYEETYPE   = "domestic"
                  UTYPE       = "account"
                  ACCOUNTNAME = PAYEE<4,1,1>
                  ACCOUNTNBR  = PAYEE<5,1,1>
                  BSB         = "932000"
               CASE TYPE = 3
                  PAYEETYPE   = "domestic"
                  UTYPE       = "account"
                  ACCOUNTNAME = PAYEE<4,1,1>
                  ACCOUNTNBR  = PAYEE<6,1,1>
                  BSB         = PAYEE<5,1,1>
               CASE TYPE = 4
               CASE TYPE = 5
               CASE TYPE = 6
               CASE TYPE = 7
               CASE TYPE = 8
                  PAYEETYPE   = "payID"
               CASE TYPE = 9
                  PAYEETYPE   = "domestic"
                  IF PAYEE<15> # "" THEN
                     UTYPE       = "payId"
                     PAYID    = PAYEE<16,1,1>
                     PAYNAME  = PAYEE<17,1,1>
                     PAYTYPE  = PAYEE<15,1,1>
                     BEGIN CASE
                        CASE PAYTYPE = "AUBN"
                           PAYTYPE = "ABN"
                        CASE PAYTYPE = "EMAL"
                           PAYTYPE = "EMAIL"
                        CASE PAYTYPE = "TELI"
                           PAYTYPE = "TELEPHONE"
                        CASE PAYTYPE = "ORGN"
                           PAYTYPE = "ORG_IDENTIFIER"
                     END CASE
                  END ELSE
                     UTYPE       = "account"
                     ACCOUNTNAME = PAYEE<4,1,1>
                     ACCOUNTNBR  = PAYEE<6,1,1>
                     BSB         = PAYEE<5,1,1>
                  END
            END CASE
            *
            LINE = atID:MARKER
            LINE:= PAYEE.ID:MARKER
            LINE:= NICKNAME:MARKER
            LINE:= PAYEETYPE:MARKER
            LINE:= BILLERCODE:MARKER
            LINE:= BILLERNAME:MARKER
            LINE:= CRN:MARKER
            LINE:= UTYPE:MARKER
            LINE:= ACCOUNTNAME:MARKER
            LINE:= ACCOUNTNBR:MARKER
            LINE:= BSB:MARKER
            LINE:= CARD:MARKER
            LINE:= PAYID:MARKER
            LINE:= PAYNAME:MARKER
            LINE:= PAYTYPE:MARKER
            LINE:= IPAY.ID:MARKER
            LINE:= INTNICKNAM :MARKER
            LINE:= INTACCTNBR :MARKER
            LINE:= INTBANKADR1:MARKER
            LINE:= INTBANKADR2:MARKER
            LINE:= INTBANKADR3:MARKER
            LINE:= INTBANKADR4:MARKER
            LINE:= INTBANKNAM :MARKER
            LINE:= INTBANKCDE :MARKER
            LINE:= INTBANKBIC :MARKER
            LINE:= INTCHIPNBR :MARKER
            LINE:= INTCOUNTRY :MARKER
            LINE:= INTWIRENBR :MARKER
            LINE:= INTENTITYID:MARKER
            LINE:= INTROUTENBR:MARKER
            LINE:= INTSORTCDE :MARKER
            LINE:= INTBENECTY :MARKER
            LINE:= INTBENEMSG :MARKER
            LINE:= INTBENENAM :MARKER
            LINE:= INTDESCRIP :MARKER
         END
         IF PAYLOAD # "" THEN PAYLOAD := F.MARK
         PAYLOAD := LINE
      NEXT D
      *
      * -----------------------------------------------------
      *
      EOD = DCOUNT(FOREIGN.INDEX, @FM)
      FOR D = 1 TO EOD
         IPAY.ID = FOREIGN.INDEX<D>
         READ INTPAYEE FROM INTERNATIONAL.PAYEE, IPAY.ID THEN
            IF INF.LOGGING THEN
               LOG.MSG = "   .) loading (I) ":IPAY.ID
               CALL uLOGGER(1, LOG.KEY:LOG.MSG)
            END
            *
            PAYEE.ID    = ""
            NICKNAME    = ""
            PAYEETYPE   = "international"
            BILLERCODE  = ""
            BILLERNAME  = ""
            CRN         = ""
            UTYPE       = ""
            ACCOUNTNAME = ""
            ACCOUNTNBR  = ""
            BSB         = ""
            CARD        = ""
            PAYID       = ""
            PAYNAME     = ""
            PAYTYPE     = ""
            INTNICKNAM  = ""
            INTACCTNBR  = ""
            INTBANKADR1 = ""
            INTBANKADR2 = ""
            INTBANKADR3 = ""
            INTBANKADR4 = ""
            INTBANKNAM  = ""
            INTBANKCDE  = ""
            INTBANKBIC  = ""
            INTCHIPNBR  = ""
            INTCOUNTRY  = ""
            INTWIRENBR  = ""
            INTENTITYID = ""
            INTROUTENBR = ""
            INTSORTCDE  = ""
            INTBENECTY  = ""
            INTBENEMSG  = ""
            INTBENENAM  = ""
            INTDESCRIP  = ""
            *
            INTNICKNAM = INTPAYEE<3,1,1>
            INTACCTNBR = INTPAYEE<7,1,1>     ;* --scrubnumber
            INTBANKADR1= INTPAYEE<11,1,1>
            INTBANKADR2= INTPAYEE<13,1,1> 
            INTBANKADR3= INTPAYEE<14,1,1> 
            INTBANKADR4= INTPAYEE<15,1,1>
            INTBANKNAM = INTPAYEE<10,1,1>
            INTBANKCDE = INTPAYEE<5,1,1>
            INTBANKBIC = INTPAYEE<6,1,1>
            INTCHIPNBR = ""   ;* ??? TBA
            INTCOUNTRY = INTPAYEE<4,1,1>
            INTWIRENBR = ""   ;* ??? TBA
            INTENTITYID= ""   ;* ??? TBA
            INTROUTENBR= ""   ;* ??? TBA
            INTSORTCDE = ""   ;* ??? TBA
            INTBENECTY = INTPAYEE<27,1,1>
            INTBENEMSG = INTPAYEE<28,1,1> : INTPAYEE<29,1,1>   ;* --scrubstring to "*"
            INTBENENAM = INTPAYEE<16,1,1>
            INTDESCRIP = INTPAYEE<26,1,1>
            *
            LINE = atID:MARKER
            LINE:= PAYEE.ID:MARKER
            LINE:= NICKNAME:MARKER
            LINE:= PAYEETYPE:MARKER
            LINE:= BILLERCODE:MARKER
            LINE:= BILLERNAME:MARKER
            LINE:= CRN:MARKER
            LINE:= UTYPE:MARKER
            LINE:= ACCOUNTNAME:MARKER
            LINE:= ACCOUNTNBR:MARKER
            LINE:= BSB:MARKER
            LINE:= CARD:MARKER
            LINE:= PAYID:MARKER
            LINE:= PAYNAME:MARKER
            LINE:= PAYTYPE:MARKER
            LINE:= IPAY.ID:MARKER
            LINE:= INTNICKNAM :MARKER
            LINE:= INTACCTNBR :MARKER
            LINE:= INTBANKADR1:MARKER
            LINE:= INTBANKADR2:MARKER
            LINE:= INTBANKADR3:MARKER
            LINE:= INTBANKADR4:MARKER
            LINE:= INTBANKNAM :MARKER
            LINE:= INTBANKCDE :MARKER
            LINE:= INTBANKBIC :MARKER
            LINE:= INTCHIPNBR :MARKER
            LINE:= INTCOUNTRY :MARKER
            LINE:= INTWIRENBR :MARKER
            LINE:= INTENTITYID:MARKER
            LINE:= INTROUTENBR:MARKER
            LINE:= INTSORTCDE :MARKER
            LINE:= INTBENECTY :MARKER
            LINE:= INTBENEMSG :MARKER
            LINE:= INTBENENAM :MARKER
            LINE:= INTDESCRIP :MARKER
         END
         IF PAYLOAD # "" THEN PAYLOAD := F.MARK
         PAYLOAD := LINE
      NEXT D
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
