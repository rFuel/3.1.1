      SUBROUTINE SR.TERMDEPOSITS ( ERR, atID, REPLY )
$INCLUDE I_Prologue
      *
      ERR      = ""
      F.MARK   = "<fm>"
      V.MARK   = "<vm>"
      S.MARK   = "<sm>"
      LOG.KEY  = "CDR-OB":@FM
      REPLY = ""
      IF INF.LOGGING THEN
         LOG.MSG = "   .) SR.TERMDEPOSITS for ":atID:" ------------------------------"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      ERR = "(GETACCOUNTS) Bad or Missing Parameters"
      IF atID = ""  THEN GO END..SRTN
      ERR = ""
      CALL SR.FILE.OPEN (ERR, "TRAN", TRAN) ; IF ERR # "" THEN GO END..SRTN
      CALL SR.FILE.OPEN (ERR, "AFFP", AFFP   ) ; IF ERR # "" THEN GO END..SRTN
      *
      READ TREC FROM TRAN, atID ELSE
         IF INF.LOGGING THEN
            LOG.MSG = "      .) ":atID:" has no TRAN record."
            CALL uLOGGER(1, LOG.KEY:LOG.MSG)
         END
         GO END..SRTN
      END
      *
      KEYS = TREC<7>
      EOI  = DCOUNT(KEYS, @VM)
      IF INF.LOGGING THEN
         LOG.MSG = "      .) ":EOI:" TD's found"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      FOR I = 1 TO EOI
         KY = KEYS<1,I>
         IF INF.LOGGING THEN
            LOG.MSG = "      .) loading P":KY:" and I":KY
            CALL uLOGGER(1, LOG.KEY:LOG.MSG)
         END
         READ PREC FROM AFFP, "P":KY ELSE
            IF INF.LOGGING THEN
               LOG.MSG = "      .) P":KY:" missing from AFFP."
               CALL uLOGGER(1, LOG.KEY:LOG.MSG)
            END
            CONTINUE
         END
         READ IREC FROM AFFP, "I":KY ELSE
            IF INF.LOGGING THEN
               LOG.MSG = "      .) I":KY:" missing from AFFP."
               CALL uLOGGER(1, LOG.KEY:LOG.MSG)
            END
            CONTINUE
         END
         *
         PINST = PREC<3,1,1>
         IF PINST="2" THEN
            PINST="ROLLED_OVER"
         END ELSE
            PINST="PAID_OUT_AT_MATURITY"
         END
         PINST = "(Principal) ":PINST
         IINST = IREC<3,1,1>
         IF IINST="2" THEN
            IINST="ROLLED_OVER"
         END ELSE
            IINST="PAID_OUT_AT_MATURITY"
         END
         IINST = "(Interest) ":IINST
         REPLY<1,-1> = OCONV(PREC<1,1,1>, "D4-")
         REPLY<2,-1> = OCONV(PREC<1,2,1>, "D4-")
         REPLY<3,-1> = PINST:" ":IINST
         REPLY<4,-1> = OCONV(PREC<7,1,1>, "MD2")
      NEXT I
      * --------------------------------------------------------
END..SRTN:
      IF INF.LOGGING THEN
         LOG.MSG = "   .) SR.TERMDEPOSITS  finished ------------------------------"
         CALL uLOGGER(1, LOG.KEY:LOG.MSG)
      END
      RETURN
      * --------------------------------------------------------
   END
