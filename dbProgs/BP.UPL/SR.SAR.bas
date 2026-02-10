      SUBROUTINE SR.SAR (REPLY, MAT IN.STRINGS)
$INCLUDE I_Prologue
      *
      * SAR:   Select and read
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
      * Example {cmd=SELECT MYFILE = "123"}  {file=MYFILE}      
      *                                                         
      * Use {atr=-1} to return ID's only                        
      
      CALL uLOGGER(0, LOG.KEY:"[SR.SAR] -------------------------------------------------------")
      
      REPLY = "{ANS=ok}"
      ERR = ""
      CALL SR.ELEMENTS(ERR, cmd.string, cmd, exec, subr, args, sel, file, item, atr, mv, sv, datum, protocol)
      ERR=""; RDIO=""
      DIM CALL.STRINGS(20) ; MAT CALL.STRINGS = ""
      closeBrace = "}"
      BaseArg = "{file=":file:closeBrace
      IF atr # "" THEN BaseArg := "{atr=":atr:closeBrace
      IF  mv # "" THEN BaseArg := "{mv=":mv:closeBrace
      IF  sv # "" THEN BaseArg := "{sv=":sv:closeBrace
      BaseArg := "{item="
      CONVERT "{" TO "" IN BaseArg
      CONVERT "}" TO @VM IN BaseArg
***   CONVERT "=" TO @SM IN BaseArg
      
      EXECUTE cmd ,OUT > JUNK, //SELECT. > SL
      CONVERT @FM TO "" IN JUNK
      CALL uLOGGER(0, LOG.KEY:"Execute [":cmd:"]")
      CALL uLOGGER(0, LOG.KEY:" Result [":JUNK:"]")
      
      CTR=0
      Ans = ""
      LOOP
         READNEXT ID FROM SL ELSE EXIT
         Rec = ""
         IF atr => 1 OR atr = "" THEN
            CALL.STRINGS(1) = BaseArg : ID : @VM
            CALL SR.RDI (Rec, MAT CALL.STRINGS)
            *
            IF UPCASE(Rec[1,5]) = "{ANS=" THEN 
               Rec = Rec[6,LEN(Rec)]
               Rec = Rec[1,LEN(Rec)-1]
               * extract <atr, mv, sv>
            END
            CALL uLOGGER(0, LOG.KEY:"   >> RDI [":file:"], [":ID:"] >> [":Rec:"]")
         END
         * <km> is the  "key marker"                              
         * <rt> is the "record terminator"                        
         * Rec  is interspersed with <fm> <vm> <sm> from SR.RDI   
         Ans := ID:"<km>":Rec:"<rt>"
         CTR+=1
      REPEAT
      IF atr < 1 OR atr # "" THEN
          CALL uLOGGER(0, LOG.KEY:"   >> SR.SAR [":file:"] selected and [":CTR:"] IDs returned")
      END
      REPLY = Ans
      RETURN
   END
