      SUBROUTINE SR.SCRAMBLESTRING ( OLDSTRING, NEWSTRING )
      *
      * Scramble alphabets in uppercase and lowercase
      *
      IF OLDSTRING="" THEN NEWSTRING=OLDSTRING; RETURN
      *
      LX = LEN(OLDSTRING)+1
      I  = 0
      SCRUBSTRING = OLDSTRING
      NEWSTRING = ""
      MASK      = "*"
      OLDUPPER  = "ABCDEFHIJKLMNOPQRSTUVWXYZ1234567890"
      OLDLOWER  = DOWNCASE(OLDUPPER)
      SYZ       = LEN(OLDUPPER)
      RAND      = RND(8)+1
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
      LX = LEN(SCRUBSTRING)+1
      NEWSTRING = ""
      LOOP
         I+=1
      WHILE I < LX
         CHR = SCRUBSTRING[I,1]
         IDX = INDEX(OLDUPPER, CHR, 1)
         IF IDX > 0 THEN
            USEOLD = OLDUPPER
         END ELSE
            IDX = INDEX(OLDLOWER, CHR, 1)
            IF IDX = 0 THEN
               USEOLD = ""
            END ELSE
               USEOLD = OLDLOWER
            END
         END
         IF USEOLD # "" THEN
            NDX = IDX + RAND
            IF NDX > SYZ THEN NDX = NDX - SYZ
            CHR = USEOLD[NDX, 1]
         END
         NEWSTRING:= CHR
      REPEAT
      RETURN
   END

