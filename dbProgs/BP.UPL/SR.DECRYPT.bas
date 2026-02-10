      SUBROUTINE SR.DENCRYPT (INSTRING, ENCSEED, PLAINTEXT)
$INCLUDE I_Prologue
      *
      * To do the same as Cipher.Decrypt
      *
      * -------------------------------------------------------------------------
      KEYBOARD25 = "!|#$%&()*+,-.0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[]{}^^_abcdefghijklmnopqrstuvwxyz"
      KEYBOARD18 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._*- "
      KEYBOARD = KEYBOARD25
      COMMA = ","
      PLAINTEXT = ""
      * -------------------------------------------------------------------------
      MARKER = INSTRING[1,1]
      PARTS  = EREPLACE(INSTRING, MARKER, @FM)
      *
      ENCRYPTED = PARTS<2>
      IF ENCSEED = "" THEN ENCSEED = PARTS<3>
      LX = LEN(ENCRYPTED)
      FOR I = 1 TO LX
         ENC.CHR = ENCRYPTED[I,1]
         IF ENC.CHR = COMMA THEN CONTINUE
         TXT.POS = INDEX(ENCSEED, ENC.CHR, 1)
         IF TXT.POS = 0 THEN
            TXT.CHR = ENC.CHR
         END ELSE
            TXT.CHR = KEYBOARD[TXT.POS, 1]
         END
         PLAINTEXT := TXT.CHR
      NEXT I
      RETURN
   END

