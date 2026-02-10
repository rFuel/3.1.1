      SUBROUTINE SR.SCRUBSTRING ( OLDSTRING, NEWSTRING )
      *
      * Simply replace alphabetics with the mask
      *
      IF OLDSTRING="" THEN NEWSTRING=OLDSTRING; RETURN
      *
      LX = LEN(OLDSTRING)+1
      NEWSTRING = ""
      MASK      = "*"
      * -------------------------------------------------
      ANSWER = ""
      EOA = DCOUNT(OLDSTRING, @FM)
      FOR A = 1 TO EOA
         VAR= OLDSTRING<A>
         EOM= DCOUNT(VAR, @VM)
         FOR M = 1 TO EOM
            VAR = OLDSTRING<A,M>
            EOS = DCOUNT(VAR, @SM)
            FOR S = 1 TO EOS
               SCRUBSTRING = OLDSTRING<A,M,S>
               GOSUB SCRUBBER
               ANSWER<A,M,S> = NEWSTRING
            NEXT S
         NEXT M
      NEXT A
      NEWSTRING = ANSWER
      RETURN ;* to calling program
      * -------------------------------------------------
SCRUBBER:
      I  = 0
      LX = LEN(SCRUBSTRING)+1
      NEWSTRING = ""
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

