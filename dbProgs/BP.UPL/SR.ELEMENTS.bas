      SUBROUTINE SR.ELEMENTS(ERR, cmd.string, cmd, exec, subr, args, sel, file, item, atr, mv, sv, datum, protocol)
$INSERT I_Prologue
      ERR=""
      cmd=""; exec=""; file=""; item=""; atr=""; mv=""; sv=""; datum=""; subr=""; args=""; sel=""; protocol=""
      eom = DCOUNT(cmd.string, @VM)
      FOR m = 1 TO eom
         this.line = cmd.string<1,m,1>
         psx = INDEX(this.line, "=", 1)
         this.key = UPCASE(this.line[1, psx-1])
         this.val = this.line[psx+1, LEN(this.line)]
         BEGIN CASE
            CASE this.key = "CMD"  AND   cmd=""  ;   cmd= this.val
            CASE this.key = "EXEC" AND  exec=""  ;  exec= this.val
            CASE this.key = "SUBR" AND  subr=""  ;  subr= this.val
            CASE this.key = "CALL" AND  subr=""  ;  subr= this.val
            CASE this.key = "ARGS" AND  args=""  ;  args= cmd.string<1,m,2>
            CASE this.key = "SEL"  AND   sel=""  ;   sel= this.val
            CASE this.key = "FILE" AND  file=""  ;  file= this.val
            CASE this.key = "ITEM" AND  item=""  ;  item= this.val
            CASE this.key = "ATR"  AND   atr=""  ;   atr= this.val
            CASE this.key = "MV"   AND    mv=""  ;    mv= this.val
            CASE this.key = "SV"   AND    sv=""  ;    sv= this.val
            CASE this.key = "DATA" AND datum=""  ; datum= this.val
            CASE this.key = "PROTOCOL" AND protocol="" ; protocol=this.val
         END CASE
      NEXT m
      RETURN
   END

