      SUBROUTINE SR.EXECUTE.MSERVICE (REPLY, MSCAT, ORDER, SRTNS, PRGMS, VPOOL, MPOOL, DPOOL)
$INCLUDE I_Prologue
      * --------------------------------------------------------------------- *
      SEP = "<tm>"
      REPLY = ""
      PRECISION 9
      STX = TIME()
      LOG.KEY = MEMORY.VARS(1):@FM
      DIM CALL.STRINGS(20)  ; MAT CALL.STRINGS  = ""
      OK    = "200"
      LOG.MSG = "   SR.EXECUTE.MSERVICE Started with [":MSCAT:"]"
      IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
      * --------------------------------------------------------------------- *
      NBR.OF.ACTIONS = DCOUNT(ORDER, @FM)
      FOR S = 1 TO NBR.OF.ACTIONS
         ARR = ""
         ACT = ORDER<S,1>
         IDX = ORDER<S,2>
         THD = ORDER<S,3>
         BEGIN CASE
            CASE ACT = "S"
               SUBR = SRTNS<IDX,1>
               ARGS = SRTNS<IDX,2>
               * ------------------------------------------------------------ *
               * Substitute Data for Variables                                *
               * EVERY micro-service subroutine MUST have 20 arguments        *
               * ------------------------------------------------------------ *
               
               CALL SR.DV.SUBSTITUTIONS(VPOOL, MPOOL, DPOOL, ARGS, ARR)
               
               MAT CALL.STRINGS  = ""
               IF ARR # "" THEN
                  FOR CS = 1 TO 20
                     CALL.STRINGS(CS) = ARR<CS>
                  NEXT CS
               END
               IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:"   ------------------------------------")
               IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:"   SR.EXECUTE.MSERVICE now calling ":SUBR)
               CALL @SUBR(MAT CALL.STRINGS)
               IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:"   ------------------------------------")
               RTN.MESSAGE = CALL.STRINGS(1)
               IF RTN.MESSAGE # "" THEN
                  IF RTN.MESSAGE[1,4] = OK:"-" THEN
                     REPLY = RTN.MESSAGE
                  END ELSE
                     REPLY := "  --  ":RTN.MESSAGE
                  END
               END
            CASE ACT = "P"
               PRGM = PRGMS<IDX,1>
               KVPS = PRGMS<IDX,2>
               IF UPCASE(THD) = "T" THEN PFX = "PHANTOM " ELSE PFX = ""
               * ------------------------------------------------------------ *
               * Substitute Data for Variables                                *
               * ------------------------------------------------------------ *
               CALL SR.DV.SUBSTITUTIONS(VPOOL, MPOOL, DPOOL, KVPS, ARR)
               EXEC = PFX:PRGM
               IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:"   ------------------------------------")
               IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:"   SR.EXECUTE.MSERVICE now executing ":EXEC)
               EOARR = DCOUNT(ARR, @FM)
               IF EOARR > 0 THEN EXEC:= " "
               FOR PARAM = 1 TO EOARR
                  EXEC := ARR<PARAM>:SEP
               NEXT PARAM
               * ----------------------------------------------------------- *
               * Programs must print errors ONLY - nothing else !!!           
               * ----------------------------------------------------------- *
               EXEC = EREPLACE(EXEC, @FM, "<fm>")
               EXEC = EREPLACE(EXEC, @VM, "<vm>")
               EXEC = EREPLACE(EXEC, @SM, "<sm>")
               EXECUTE EXEC CAPTURING OUTPUT
               IF OUTPUT # "" AND UPCASE(THD) # "T" THEN
                  REPLY := "  --  ":EREPLACE(OUTPUT, @FM, " ")
               END
               IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:"   ------------------------------------")
            CASE 1
               REPLY := "  --  [FAIL] Unknown action [":ACT:"] in micro-service [":ACT:"]"
         END CASE
      NEXT S
***      IF REPLY # "" THEN
***         LOG.MSG = "   SR.EXECUTE.MSERVICE returning [":REPLY:"]"
***         IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
***      END
      ETX = TIME()
      DIFF= ETX - STX
      LOG.MSG = "   SR.EXECUTE.MSERVICE Finished with [":MSCAT:"] in ":DIFF:" seconds"
      IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:LOG.MSG)
      RETURN
   END

