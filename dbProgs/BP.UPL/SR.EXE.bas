      SUBROUTINE SR.EXE (REPLY, MAT IN.STRINGS)
$INCLUDE I_Prologue
      *
      * EXE:   Execute something
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
      LOG.KEY = MEMORY.VARS(1) : @FM
      REPLY = "{ANS=ok}"
      ERR = ""
      CALL SR.ELEMENTS(ERR, cmd.string, cmd, exec, subr, args, sel, file, item, atr, mv, sv, datum, protocol)
      *
      IF exec # "" THEN 
         EXECUTE exec CAPTURING JUNK
         JUNK = EREPLACE(JUNK, @FM , "")
         LOG.MSG = "SR.EXE executed ":exec:"  response: ":JUNK
         CALL uLOGGER(5, LOG.KEY:LOG.MSG)
      END
      *
      RETURN
   END
