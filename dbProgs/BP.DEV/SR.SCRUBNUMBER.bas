      SUBROUTINE SR.SCRUBNUMBER ( OLDNUMBER, NEWNUMBER )
      IF OLDNUMBER="" THEN NEWNUMBER=OLDNUMBER; RETURN
      *
      OLDSTRING = OLDNUMBER
      NEWNUMBER = ""
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
               ANSWER<A,M,S> = NEWNUMBER
            NEXT S
         NEXT M
      NEXT A
      NEWNUMBER = ANSWER
      RETURN ;* to calling program
      * -------------------------------------------------
SCRUBBER:
      I  = 0
      LX = LEN(SCRUBSTRING)+1
      NEWNUMBER = ""
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

