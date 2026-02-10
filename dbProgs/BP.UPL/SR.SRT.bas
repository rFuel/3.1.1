      SUBROUTINE SR.SRT (REPLY, MAT IN.STRINGS)
$INCLUDE I_Prologue
      *                                                         
      *  SRT:  subroutine caller                                
      *        The subroutine must use the MAT CALL.STRINGS     
      *        and CALL.STRINGS(1) is the return value.         
      *                                                         
      * --------------------------------------------------------
      * Equate local variables to call string locations         
      * Subroutine API:                                         
      * --------------------------------------------------------
$IFDEF isRT
      DIM IN.STRINGS(20)
$ENDIF
      EQU cmd.string   TO IN.STRINGS(1)
      * --------------------------------------------------------
      REPLY = "{ANS=ok}"
      ERR = ""
      CALL SR.ELEMENTS(ERR, cmd.string, cmd, exec, subr, args, sel, file, item, atr, mv, sv, datum, protocol)
      *
      DELIM = "<im>":@FM:"<fm>":@FM:"<vm>":@FM:"<sm>":@FM:"<tm>"
      EOD = DCOUNT(DELIM, @FM)
      INSTRING = args
      FOR D = 1 TO EOD
         REPLY    = ""
         CALL SR.GET.INSTRINGS (RTN.STRING , INSTRING , DELIM<D> , REPLY)
         INSTRING = REPLY
      NEXT D
      *
      NBR.ARGS = DCOUNT(REPLY, @FM)
      IF NBR.ARGS < 21 THEN
         DIM CALL.STRINGS(20)
         MAT CALL.STRINGS  = ""
         IF INF.LOGGING THEN CALL uLOGGER(0, "SR.SRT is preparing ":subr)
         showLIST = ""
         FOR CS = 2 TO 20
            CALL.STRINGS(CS) = REPLY<CS-1>
            showLIST := "[":CS:"> ":CALL.STRINGS(CS):"] "
         NEXT CS
         IF INF.LOGGING THEN CALL uLOGGER(0, "ARGS ":showLIST)
         IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:"   ------------------------------------")
         IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:"   Calling ":subr)
         CALL @subr(MAT CALL.STRINGS)
         REPLY = "{":CALL.STRINGS(1):"}"
         IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:"   Return: ":REPLY)
         IF INF.LOGGING THEN CALL uLOGGER(0, LOG.KEY:"   ------------------------------------")
      END ELSE
         REPLY = "{EOX}"
      END
      *
      RETURN
   END
