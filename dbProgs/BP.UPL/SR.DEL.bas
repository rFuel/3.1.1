      SUBROUTINE SR.DEL (REPLY, MAT IN.STRINGS)
$INCLUDE I_Prologue
      *
      * DEL:   Delete Item
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
      IF MEMORY.VARS(1) = "" THEN MEMORY.VARS(1) = "uplLOG"
      LOG.KEY = MEMORY.VARS(1):@FM
      
      REPLY = "{EOX}"
      ERR = ""
      CALL SR.ELEMENTS(ERR, cmd.string, cmd, exec, subr, args, sel, file, item, atr, mv, sv, datum, protocol)
      CALL SR.FILE.OPEN (ERR, file, RDIO)
      IF NOT(ERR) THEN
         READU Ans FROM RDIO, item THEN
            DEL.SW = atr#""
            DEL.SW = DEL.SW AND NUM(atr)
            DEL.SW = DEL.SW AND atr#0
            IF DEL.SW THEN 
               IF mv="" THEN mv=0
               IF sv="" THEN sv=0
               DEL Ans<atr, mv, sv>
               WRITE Ans ON RDIO, item 
               CALL uLOGGER(0, LOG.KEY:"   >> SR.DEL [":file:"] , [":item:"] <":atr:",":mv:",":sv:"> deleted.")
            END ELSE
               DELETE RDIO, item
               CALL uLOGGER(0, LOG.KEY:"   >> SR.DEL [":file:"] , [":item:"] deleted.")
            END
            REPLY = "{ANS=ok}"
         END
         RELEASE RDIO, item
      END
      *
      RETURN
   END
