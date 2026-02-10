      SUBROUTINE SR.SCRUBSTRING ( OLDSTRING, NEWSTRING )
      *
      * Simply replace alphabetics with the mask
      *
      IF OLDSTRING="" THEN NEWSTRING=OLDSTRING; RETURN
      *
      LX = LEN(OLDSTRING)+1
      I  = 0
      NEWSTRING = ""
      MASK      = "*"
      LOOP
         I+=1
      WHILE I < LX
         CHR = OLDSTRING[I,1]
         IDX = SEQ(CHR)
         IF IDX >= 65 AND IDX <= 122 THEN CHR = MASK
         NEWSTRING:= CHR
      REPEAT
      RETURN
   END
