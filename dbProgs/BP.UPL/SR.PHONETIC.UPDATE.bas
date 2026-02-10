      SUBROUTINE SR.PHONETIC.UPDATE (MAT IN.STRINGS)
$INCLUDE I_Prologue
      *
      * ============================================================================
      * API to the subroutine:
$IFDEF isRT
      DIM IN.STRINGS(20)
$ENDIF
      EQU RTN.CODE      TO IN.STRINGS(1)
      EQU BASE.FILE     TO IN.STRINGS(2)
      EQU BASE.ID       TO IN.STRINGS(3)
      EQU BASE.STRING   TO IN.STRINGS(4)
      * ============================================================================
      * Explanation:
      * ============
      * Get the soundex value for BASE.STRING and update "PHONETIC.":BASE.FILE
      * Soundex produces a value like "A1234". The layout of phonetic files are;
      * @ID = phonetic value                          (e.g. 1234)
      * 1>  Most significant letter    mv 1-n         (e.g. "A")
      * 2>  Assoc. BASE.FILE ID's      mv 1-n sv 1-n  (e.g. "12345\12546\etc..")
      * ============================================================================
      ERR = ""
      RTN.CODE = "500-"; EXISTS=0; KEEPLOCK=0
      PHONETIC.IDX = "PHONETIX"; *"PHONETIC.":BASE.FILE
      CALL SR.OPEN.CREATE(ERR, PHONETIC.IDX, "30", SDX.INDEX)
      IF ERR # "" THEN
         RTN.CODE := "SR.PHONETIC.UPDATE cannot access [PHONETIC.":BASE.FILE:"] file"
         RETURN
      END
      *
      THIS.ID = BASE.FILE:"<tm>":BASE.ID
      *
      * Get the old soundex lookup
      *
      CALL SR.ITEM.EXISTS (EXISTS, SDX.INDEX, THIS.ID, OLD.REC, KEEPLOCK)
      *
      * Create the new soundex lookup
      *
      sndx = SOUNDEX(UPCASE(BASE.STRING))
      ALPHA= sndx[1,1]
      SDXNO= sndx[2,LEN(sndx)]
      *
READ..THE..REC:
      CALL SR.ITEM.EXISTS (EXISTS, SDX.INDEX, SDXNO, SDX.REC, KEEPLOCK)
      BEGIN CASE
         CASE EXISTS=0
            SDX.REC<1> = ALPHA
            SDX.REC<2> = THIS.ID
         CASE EXISTS=1
            * remove it, in case it changes
            IF INDEX(SDX.REC<2>, THIS.ID, 1) THEN
               SDX.REC<2> = EREPLACE(SDX.REC<2>, THIS.ID, "")
               SDX.REC<2> = EREPLACE(SDX.REC<2>, @SM:@SM, @SM)
            END
            * add the new value and tidy up
            EOM = DCOUNT(SDX.REC<1>, @VM)
            FOR M = 1 TO EOM
               MSL = SDX.REC<1,M>      ; * most significant letter
RESTART:
               DONE = 0
               EOS = DCOUNT(SDX.REC<2,M>, @SM)
               FOR S = 1 TO EOS
                  KEY = SDX.REC<2,M,S>
                  IF KEY = "" AND S < EOS THEN 
                     SDX.REC = DELETE(SDX.REC,2,M,S)
                     GOTO RESTART
                  END
               NEXT S
               IF NOT(DONE) AND MSL = ALPHA THEN
                  SDX.REC<2,M,-1> = THIS.ID
                  DONE = 1
                  EXIT
               END
            NEXT M
         CASE EXISTS=2
$IFDEF isRT
            RQM
$ELSE
            NAP 250
$ENDIF
            GO READ..THE..REC
      END CASE
      *
      * Add the soundex record
      *
      DIM FILE.ARR(20)     ;MAT FILE.ARR = ""   ; FILE.ARR(1)= SDX.INDEX
      DIM REC.ARR(20)      ;MAT REC.ARR = ""    ; REC.ARR(1) = SDX.REC
      DIM ID.ARR(20)       ;MAT ID.ARR = ""     ; ID.ARR(1)  = SDXNO
      DIM WV.ARR(20)       ;MAT WV.ARR = ""     ; WV.ARR(1)  = ""
      DIM WU.ARR(20)       ;MAT WU.ARR = ""     ; WU.ARR(1)  = ""
      
      CALL SR_DBWRITER_UV (RTN.STRING, MAT REC.ARR, MAT FILE.ARR, MAT ID.ARR, MAT WV.ARR, MAT WU.ARR)
      IF RTN.STRING THEN
         RTN.CODE = RTN.STRING
         RETURN
      END
      *
      * Add the reverse record
      *
      MAT FILE.ARR = ""     ;FILE.ARR(1)= SDX.INDEX
      MAT REC.ARR = ""      ;REC.ARR(1) = ALPHA:SDXNO
      MAT ID.ARR = ""       ;ID.ARR(1)  = THIS.ID
      MAT WV.ARR = ""       ;WV.ARR(1)  = ""
      MAT WU.ARR = ""       ;WU.ARR(1)  = ""
      
      CALL SR_DBWRITER_UV (RTN.STRING, MAT REC.ARR, MAT FILE.ARR, MAT ID.ARR, MAT WV.ARR, MAT WU.ARR)
      IF RTN.STRING THEN
         RTN.CODE = RTN.STRING
         RETURN
      END
      
      RTN.CODE = "200-OK"
      RETURN
   END
