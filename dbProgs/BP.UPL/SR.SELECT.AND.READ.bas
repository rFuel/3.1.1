      SUBROUTINE SR.SELECT.AND.READ (REPLY, MAT IN.STRINGS)
$INCLUDE I_Prologue
      * --------------------------------------------------------
      * Equate local variables to call string locations         
      * Subroutine API:                                         
      * --------------------------------------------------------
$IFDEF isRT
      DIM IN.STRINGS(20)
$ENDIF
      EQU cmd.string   TO IN.STRINGS(1)
******EQU LOG.KEY      TO IN.STRINGS(2)
      * --------------------------------------------------------
      * Example {cmd=SELECT MYFILE = "123"}  {file=MYFILE}      
      
      REPLY = "{EOX}"
      file=""; item=""; atr=""; mv=""; sv=""; ERR=""; RDIO=""; cmd=""
      eom = DCOUNT(cmd.string, @VM)
      FOR m = 1 TO eom
         this.key = UPCASE(cmd.string<1,m,1>)
         this.val = cmd.string<1,m,2>
         BEGIN CASE
            CASE this.key = "CMD"  AND  cmd=""  ;   cmd= this.val
            CASE this.key = "FILE" AND file=""  ;  file= this.val
            CASE this.key = "ATR"  AND  atr=""  ;   atr= this.val
            CASE this.key = "MV"   AND   mv=""  ;    mv= this.val
            CASE this.key = "SV"   AND   sv=""  ;    sv= this.val
         END CASE
      NEXT m
      
      ** IF INF.LOGGING THEN CALL uLOGGER(5, LOG.KEY:"SelectAndRead(): Command ":cmd)
      EXECUTE cmd CAPTURING JUNK
      Ans = ""
      DIM CALL.STRINGS(20) ; MAT CALL.STRINGS = ""
      closeBrace = "}"
      BaseArg = "{file=":file:closeBrace
      IF atr # "" THEN BaseArg := "{atr=":atr:closeBrace
      IF  mv # "" THEN BaseArg := "{mv=":mv:closeBrace
      IF  sv # "" THEN BaseArg := "{sv=":sv:closeBrace
      BaseArg := "{item="
      CONVERT "{" TO "" IN BaseArg
      CONVERT "}" TO @VM IN BaseArg
      CONVERT "=" TO @SM IN BaseArg
      LOOP
         READNEXT ID ELSE EXIT
         Rec = ""
         sendToUV = BaseArg:ID:@VM
         CALL.STRINGS(1) = sendToUV
         CALL SR.RDI (Rec, MAT CALL.STRINGS)
         IF UPCASE(Rec[1,5]) = "{ANS=" THEN 
            Rec = Rec[6,LEN(Rec)]
            Rec = Rec[1,LEN(Rec)-1]
         END
         Ans := ID:"<km>":Rec:"<im>"
      REPEAT
      REPLY = Ans
      RETURN
   END
   
