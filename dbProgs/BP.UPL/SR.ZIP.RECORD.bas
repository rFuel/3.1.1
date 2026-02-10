      SUBROUTINE SR.ZIP.RECORD (MAT IN.STRINGS)
$INCLUDE I_Prologue
      * --------------------------------------------------------
      * Equate local variables to call string locations         
      * Subroutine API:                                         
      * --------------------------------------------------------
$IFDEF isRT
      DIM IN.STRINGS(20)
$ENDIF
***   DIM IN.STRINGS(2)
      EQU REPLY TO IN.STRINGS(1)
      EQU iREC  TO IN.STRINGS(2)    ; * Input  record
      EQU oREC  TO IN.STRINGS(3)    ; * Output record
      * --------------------------------------------------------
      oREC = EREPLACE(iREC, @FM , "<fm>")
      oREC = EREPLACE(oREC, @VM , "<vm>")
      oREC = EREPLACE(oREC, @SM , "<sm>")
      REPLY= ""
      * --------------------------------------------------------
      *
      RETURN
   END
