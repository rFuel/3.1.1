      SUBROUTINE SR.SCRUBNUMBER ( OLDNUMBER, NEWNUMBER )
      IF OLDNUMBER="" THEN NEWNUMBER=OLDNUMBER; RETURN
      *
      LX = LEN(OLDNUMBER)+1
      I  = 0
      NEWNUMBER = ""
      MASK      = "*"
      LOOP
         I+=1
      WHILE I < LX
         NBR = OLDNUMBER[I,1]
         CHR = NBR
         IDX = SEQ(NBR)
         IF IDX >= 48 AND IDX <= 57 THEN
            CHR = RND(9)
            IF CHR = NBR THEN I -= 1 ; CONTINUE
         END
         NEWNUMBER:= CHR
      REPEAT
      RETURN
   END
