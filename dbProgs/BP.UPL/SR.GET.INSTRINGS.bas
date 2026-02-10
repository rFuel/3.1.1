      SUBROUTINE SR.GET.INSTRINGS (RTN.STRING, SENTENCE, SEP, CMD)
$INCLUDE I_Prologue
      RTN.STRING = ""
      CMD = ""
      INSTR = SENTENCE
      *-----------------------------------------------------------
      * DO NOT convert or swap SEP for @fm @vm or @sm             
      *     - the data can have @fm @vm or @sm in it !!!!!!!      
      *     - Note: SEP can be a string such as <tm> [!] {*} etc. 
      *-----------------------------------------------------------
      LOOP
         POS = INDEX(INSTR, SEP, 1) - 1
         IF POS <= 0 THEN
            IF INSTR = "" THEN EXIT
            CMD<-1> = INSTR
            INSTR   = ""
            EXIT
***         INSTR = INSTR[POS+1+LEN(SEP), LEN(INSTR)]
***         INSTR = TRIMF(INSTR)
***         CONTINUE
         END
         TMP = INSTR[1,POS]
         TMP = TRIMF(TMP)
         CMD<-1> = TMP
         INSTR = INSTR[POS+1+LEN(SEP), LEN(INSTR)]
         INSTR = TRIMF(INSTR)
      REPEAT
      IF CMD = "" THEN CMD = SENTENCE
      IF INSTR # "" THEN CMD<-1> = INSTR
      RETURN
   END
