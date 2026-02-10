      SUBROUTINE SCRUB.DATA(GOODCHARS, RECORD)
      TREC = RECORD
      EOA = DCOUNT(TREC, @FM)
      FOR A = 1 TO EOA
         TMPA = TREC<A>
         EOM = DCOUNT(TMPA, @VM)
         FOR M = 1 TO EOM
            TMPM = TMPA<1,M>
            EOS = DCOUNT(TMPM, @SM)
            FOR S = 1 TO EOS
               TMPS = TMPM<1,1,S>
               CONVERT GOODCHARS TO '' IN TMPS
               IF LEN(TMPS) THEN TREC<A,M,S> = "ENCRYTED"
            NEXT S
         NEXT M
      NEXT A
      RECORD = TREC
      RETURN
   END
