      SUBROUTINE SR.WRI (REPLY, MAT IN.STRINGS)
$INCLUDE I_Prologue
      *
      * WRI:   WRite Item
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
      WRTIO = "" ; CALL SR.FILE.OPEN(ERR, file, WRTIO)
      IF ERR = "" AND item # "" THEN
         READU REC FROM WRTIO, item ELSE REC = ""
         IF atr THEN
            IF mv THEN
               IF sv THEN
                  REC<atr,mv,sv> = datum
               END ELSE
                  REC<atr,mv> = datum
               END
            END ELSE
               REC<atr> = datum
            END
         END ELSE
            REC = datum
         END
         is = "<is>"
         SBO= "["
         SBC= "]"
         fm = SBO:SBO:"fm":SBC:SBC
         vm = SBO:SBO:"vm":SBC:SBC
         sm = SBO:SBO:"sm":SBC:SBC
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
         IF ERR #"" THEN REPLY = "{ANS=":file:" open error: ":ERR:"}"
         IF item="" THEN REPLY = "{ANS=no-item-provided}"
      END
      *
      RETURN
   END
