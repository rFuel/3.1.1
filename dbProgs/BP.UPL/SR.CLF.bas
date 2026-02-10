      SUBROUTINE SR.CLF (REPLY, MAT IN.STRINGS)
$INCLUDE I_Prologue
      *
      * CLF:   Clear File
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
      REPLY = "{EOX}"
      ERR = ""
      CALL SR.ELEMENTS(ERR, cmd.string, cmd, exec, subr, args, sel, file, item, atr, mv, sv, datum, protocol)
      CALL SR.FILE.OPEN(ERR, file, IOFILE)
      IF ERR # "" THEN
         CALL uLOGGER(0, MEMORY.VARS(1):@FM:"Cannot CLEAR ":file:" - it does not exist")
         RETURN
      END
      IF sel = "" THEN
         EXECUTE "CLEAR-FILE ":file CAPTURING JUNK
      END ELSE
         IOFILE = "" ; CALL SR.FILE.OPEN(ERR, file, IOFILE)
         IF ERR THEN
            CRT " --> " "R#12":"  SR.CLF Error ":file:" not found"
         END ELSE
            EXECUTE sel CAPTURING JUNK
            CNT = @SELECTED
            LOOP
               READNEXT ID ELSE EXIT
               DELETE IOFILE, ID
            REPEAT
            IF INF.LOGGING THEN CALL uLOGGER(0, MEMORY.VARS(1):@FM:CNT:" records deleted from ":file)
         END
      END
      REPLY = "{ANS=ok}"
      RETURN
   END
