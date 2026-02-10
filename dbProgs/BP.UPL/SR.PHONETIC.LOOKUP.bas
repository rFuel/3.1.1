      SUBROUTINE SR.PHONETIC.LOOKUP (MAT IN.STRINGS)
$INCLUDE I_Prologue
      *
      * ============================================================================
      * API to the subroutine:
$IFDEF isRT
      DIM IN.STRINGS(20)
$ENDIF
      EQU RTN.CODE      TO IN.STRINGS(1)
      EQU BASE.FILE     TO IN.STRINGS(2)
      EQU BASE.STRING   TO IN.STRINGS(3)
      EQU MATCHING.VALS TO IN.STRINGS(4)
      * ============================================================================
      * Explanation:
      * ============
      * Get the soundex value for BASE.STRING and lookup "PHONETIC.":BASE.FILE
      * Soundex produces a value like "A1234". The layout of phonetic files are;
      * @ID = phonetic value                          (e.g. 1234)
      * 1>  Most significant letter    mv 1-n         (e.g. "A")
      * 2>  Assoc. BASE.FILE ID's      mv 1-n sv 1-n  (e.g. "12345\12546\etc..")
      * ============================================================================
      ERR = ""; MATCHING.VALS = ""
      RTN.CODE = "500-"; EXISTS=0; KEEPLOCK=0
      PHONETIC.IDX = "PHONETIC.":BASE.FILE
      *
      CALL SR.OPEN.CREATE(ERR, PHONETIC.IDX, "30", SDX.INDEX)
      IF ERR # "" THEN
         RTN.CODE := "SR.PHONETIC.UPDATE cannot access [PHONETIC.":BASE.FILE:"] file"
         RETURN
      END
      *
      sndx = SOUNDEX(UPCASE(BASE.STRING))
      ALPHA= sndx[1,1]
      SDXNO= sndx[2,LEN(sndx)]
      *
      * Get the soundex lookup
      *
      CALL SR.ITEM.EXISTS (EXISTS, SDX.INDEX, SDXNO, SDX.REC, KEEPLOCK)
      BEGIN CASE
         CASE EXISTS=0
            RTN.CODE = "200-OK"
         CASE EXISTS=1
            LOCATE ALPHA IN SDX.REC<1> SETTING FND THEN
               ANS = ""
               IF BASE.FILE = "*" THEN
                  ANS = SDX.REC<2, FND>
               END ELSE
                  EOS = DCOUNT(SDX.REC<2,FND>, @SM)
                  FOR S = 1 TO EOS
                     IF FIELD(SDX.REC<2,FND,S>, "<tm>", 1) = BASE.FILE THEN
                        ANS := SDX.REC<2,FND,S>:@SM
                     END
                  NEXT S
               END
               EOS = DCOUNT(ANS, @SM)
               FOR S = 1 TO EOS
                  IF ANS<1,1,S> # "" THEN MATCHING.VALS := "<sm>":ANS<1,1,S>
               NEXT S
               MATCHING.VALS = MATCHING.VALS[5,LEN(MATCHING.VALS)]
            END         
         CASE EXISTS=2
            RTN.CODE := "200-Record [":BASE.ID:"] in ":BASE.FILE:" is currently locked."
            RETURN
      END CASE
      *
      RTN.CODE = "200-OK"
      RETURN
   END
