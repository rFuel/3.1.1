      SUBROUTINE SR.RDI (REPLY, MAT IN.STRINGS)
$INCLUDE I_Prologue
      *
      * RDI:   Read Item
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
      ERR = ""; RDIO= ""
$IFDEF isRT
      file = protocol:">>":file
$ENDIF
      CALL SR.FILE.OPEN (ERR, file, RDIO)
      IF ERR = "" THEN
         READ Ans FROM RDIO, item THEN
            IF atr #"" AND NUM(atr) THEN 
               Ans = Ans<atr>
               IF  mv #"" AND NUM(mv) THEN 
                  Ans = Ans<1, mv>
                  IF  sv #"" AND NUM(sv) THEN Ans = Ans<1, 1, sv>
               END
            END
            Ans = EREPLACE(Ans, @SM, "<sm>")
            Ans = EREPLACE(Ans, @VM, "<vm>")
            Ans = EREPLACE(Ans, @FM, "<fm>")
         END ELSE
            Ans = ""
         END
      END ELSE
         Ans = ERR
      END
      REPLY = "{ANS=":Ans:"}"
      *
      RETURN
   END
