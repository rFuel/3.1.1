      SUBROUTINE SR.ENCRYPT (ENCR, INSTR)
$INCLUDE I_Prologue
      *
      * To do the same as Cipher.Encrypt 
      *
      REPL = ""
      ENCR = ""
      SEP  = TILDE
      IF INDEX(INSTR, TILDE, 1) THEN
         SBO= "["
         SBC= "]"
         CBO= "{"
         CBC= "}"
         ALTS = "`@#$%^^&*().,/<>?;:-_=+|":SBO:SBC:CBO:CBC
         ALX  = LEN(ALTS)
         ACX  = 0
         FOR LL = 1 TO ALX
            REPL = ALTS[LL,1]
            IF NOT(INDEX(INSTR, REPL, 1)) THEN ACX = LL; EXIT
         NEXT LL
         IF ACX = 0 THEN 
            REPL = CHAR(9)
***         LOOP WHILE INDEX(INSTR, TILDE, 1) DO
***            CONVERT TILDE TO REPL IN INSTR
***         REPEAT
***         INSTR = INSTR : TILDE : REPL
***         RETURN  ;* cannot encrypt this string
         END
***      LOOP WHILE INDEX(INSTR, TILDE, 1) DO
***         CONVERT TILDE TO REPL IN INSTR
***      REPEAT
         SEP = REPL
      END
      *
      KEYBOARD25 = "!|#$%&()*+,-.0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[]{}^^_abcdefghijklmnopqrstuvwxyz"
      KEYBOARD18 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._*- "
      KEYBOARD = KEYBOARD25
      ORDER    = KEYBOARD
      ORIGINAL = ORDER
      SEED     = ""
      LOOP
         LX = LEN(ORDER)
      WHILE LX > 0 DO
         RI    = RND(LX)
         IF LX = 1 THEN RI = 1
         IF RI > 0 THEN
            CHR   = ORDER[RI, 1]
            SEED := CHR
            ORDER = ORDER[1,RI-1] : ORDER[RI+1, LX]
         END
      REPEAT
      *
      LX = LEN(INSTR)
      FOR X = 1 TO LX
         CHR = INSTR[X, 1]
         POS = INDEX(ORIGINAL, CHR, 1)
         IF POS < 1 THEN
            ENCR := CHR
         END ELSE
            ENCR := SEED[POS, 1]
         END
      NEXT X
***   ENCR := TILDE : SEED : TILDE : REPL
      ENCR = SEP : ENCR : SEP : SEED
      RETURN
   END

