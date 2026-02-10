      SUBROUTINE SR.LBP (REPLY, MAT IN.STRINGS)
$INCLUDE I_Prologue
      *
      * LBP:   WRite programs to a BP file 
      *        Redundant - see SLBP instead
      *
      * --------------------------------------------------------
      * Equate local variables to call string locations         
      * Subroutine API:                                         
      * --------------------------------------------------------
$IFDEF isRT
      DIM IN.STRINGS(20)
$ENDIF
      EQU cmd.string   TO IN.STRINGS(1)
      REQUEST = cmd.string
      * --------------------------------------------------------
      REPLY = "{EOX}"
      cmd.string = EREPLACE(cmd.string, "{", "")
      cmd.string = EREPLACE(cmd.string, "}", @VM)
      cmd.string = EREPLACE(cmd.string, "<bo>", "{")
      cmd.string = EREPLACE(cmd.string, "<bc>", "}")
      IF UPCASE(cmd.string<1,1>) # "LBP" THEN 
         REPLY = "<<FAIL>> Bad metabasic request ":REQUEST
         CALL uLOGGER(0, LOG.KEY:REPLY)
         RETURN
      END
      CALL SR.ELEMENTS(ERR, cmd.string, cmd, exec, subr, args, sel, file, item, atr, mv, sv, datum, protocol)
      WRTIO = "" ; CALL SR.FILE.OPEN(ERR, file, WRTIO)
      IF NOT(ERR) AND item # "" THEN
         is = "<is>"
         SBO= "["
         SBC= "]"
         fm = SBO:SBO:"fm":SBC:SBC
         vm = SBO:SBO:"vm":SBC:SBC
         sm = SBO:SBO:"sm":SBC:SBC
         READU REC FROM WRTIO, item ELSE REC = ""
         REC = datum
         REPLY = "{ANS=ok}"
         REC = EREPLACE(REC, is, "=")
         REC = EREPLACE(REC, fm, @FM)
         REC = EREPLACE(REC, vm, @VM)
         REC = EREPLACE(REC, sm, @SM)
$IFDEF isRT
         WRITE REC ON WRTIO, item ON ERROR
            REPLY = "{ANS=fail}"
         END
$ELSE
         WRITE REC ON WRTIO, item ELSE
            REPLY = "{ANS=fail}"
         END
$ENDIF
      END ELSE
         IF ERR THEN 
            REPLY = "{ANS=":ERR
            IF file="" THEN REPLY := " no-file-provided"
            IF item="" THEN REPLY := " no-item-provided"
            REPLY := "}"
         END
      END
      *
      RETURN
   END
