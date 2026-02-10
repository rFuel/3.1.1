$INCLUDE I_Prologue
      * -----------------------------------------------------------------------
      * Usage: KB.PREPARE.TRANEXT                                              
      *      : This will create reconciliation records for a product(s)        
      * -----------------------------------------------------------------------
      PROG = "KB.PREPARE.TRANEXT"
      IF MEMORY.VARS(1) =  "" THEN MEMORY.VARS(1) = PROG
      LOG.KEY = MEMORY.VARS(1):@FM
      LOG.LEVEL = 3
      ERR = ""
      MSG = PROG:" starting ======================================="
      PROMPT ""
      * --------------------------------------------------------------------
$IFDEF isUV
      CMD = @SENTENCE
$ENDIF
      *
$IFDEF isUD
      CMD = @SENTENCE
$ENDIF
      *
$IFDEF isRT
      CMD = SENTENCE
$ENDIF
      * --------------------------------------------------------------------
      CONVERT " " TO @FM IN CMD
      LOCATE(PROG, CMD; POS) ELSE STOP "Unknown command."
      FOR I = 1 TO POS
         CMD = DELETE(CMD, 1, 0, 0)
      NEXT I
      PRD.LIST = CMD
      MSG.ISO  = PROG:"*":PRD.LIST:" "
      PRD.LIST = EREPLACE(PRD.LIST, ",", @FM)
      EOP = DCOUNT(PRD.LIST, @FM)
      IF EOP>0 THEN 
         EXECUTE "PHANTOM KB.TRAN.RECON FILE=RESET" CAPTURING OUTPUT
         FOR P = 1 TO EOP
            CMD = "PHANTOM KB.TRAN.RECON FILE=RFUELPILOT*TRAN PRODUCT=":PRD.LIST<P>
            EXECUTE CMD CAPTURING OUTPUT
            FOR T = 1 TO 10
               CMD = "PHANTOM KB.TRAN.RECON FILE=RFUELPILOT*TRAN":T:".EXT PRODUCT=":PRD.LIST<P>
               EXECUTE CMD CAPTURING OUTPUT
            NEXT T
         NEXT P
      END
      STOP
   END
