      SUBROUTINE SR.CROSS.FILE.UPDATE (REPLY, MAT IN.STRINGS)
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
      EQU OTHER.FILE    TO IN.STRINGS(4)
      EQU CROSS.MAP     TO IN.STRINGS(5)
      EQU kvSEP         TO IN.STRINGS(6)
      EQU NEW.LINE      TO IN.STRINGS(7)
      * ============================================================================
      * Explanation:
      * ============
      * Use BASE.ID to read the record from BASE.FILE.
      * Use the CROSS.MAP to take data from BASE record and update the OTHER.FILE
      * The CROSS.MAP absolutely MUST have a map to attribute 0 (@ID) in line 1
      * The CROSS.MAP looks like this:-
      * #from  ,  to    ,iconv   ,oconv
      * 2_1_1  ,  0_0_0 ,        ,
      * 7_1_1  ,  3_2_1 ,        ,
      * ============================================================================
      *
      
      DIM REC.ARR(20)  ;MAT REC.ARR = ""
      DIM FILE.ARR(20) ;MAT FILE.ARR = ""
      DIM ID.ARR(20)   ;MAT ID.ARR = ""
      DIM WV.ARR(20)   ;MAT WV.ARR = ""
      DIM WU.ARR(20)   ;MAT WU.ARR = ""
      
      * ============================================================================
      RTN.CODE = "500-"; ERR = ""; EXISTS=0; KEEPLOCK=0
      * ============================================================================
      
      CALL SR.OPEN.CREATE (ERR, BASE.FILE, "30", BASEIO)
      IF ERR THEN
         RTN.CODE := "Cannot Open/Create ":BASE.FILE
         RETURN
      END
      CALL SR.ITEM.EXISTS (EXISTS, BASEIO, BASE.ID, BASE.REC, KEEPLOCK)
      BEGIN CASE
         CASE EXISTS=0
            RTN.CODE := "Cannot find ":BASE.ID:" in ":BASE.FILE
            RETURN
         CASE EXISTS=2
            RTN.CODE := "Record [":BASE.ID:"] in ":BASE.FILE:" is currently locked."
            RETURN
      END CASE
      * ============================================================================
      CALL SR.OPEN.CREATE (ERR, OTHER.FILE, "30", OTHERIO)
      IF ERR THEN
         RTN.CODE := "Cannot Open/Create ":BASE.FILE
         RETURN
      END
      
      CALL SR.GET.INSTRINGS (RTN.STRING, CROSS.MAP, NEW.LINE, RTN.VALUE)
      MAP = "" ; ARR = RTN.VALUE
      EOL = DCOUNT(ARR, @FM)
      FOR L = 1 TO EOL
         LINE = ARR<L>
         IF TRIMF(LINE)[1,1] = "#" THEN CONTINUE
         INP.LINE = LINE
         CALL SR.GET.INSTRINGS (RTN.STRING, INP.LINE, kvSEP, LINE)
         LINE = EREPLACE(LINE, @FM, @VM)
         IF MAP = "" THEN
            IF FIELD(LINE<1,2>, "_", 1) # 0 THEN
               RTN.CODE := " Sorry, first useable map line MUST map 'To' attribute 0."
               RETURN
            END
         END
         MAP<-1> = LINE
      NEXT L
      
      fA = FIELD(MAP<1,1>, "_", 1)        ; * from Attribute
      fM = FIELD(MAP<1,1>, "_", 2)        ; * from Multi-value
      fS = FIELD(MAP<1,1>, "_", 3)        ; * from Sub-value
      IF fA = 0 THEN
         OTHER.ID = BASE.ID
      END ELSE
         OTHER.ID = BASE.REC<fA, fM, fS>
      END
      
      CALL SR.ITEM.EXISTS (EXISTS, OTHERIO, OTHER.ID, OTHER.REC, KEEPLOCK)
      BEGIN CASE
         CASE EXISTS=0
            OTHER.REC = ""
         CASE EXISTS=2
            RTN.CODE := "Record [":BASE.ID:"] in ":BASE.FILE:" is currently locked."
            RETURN
      END CASE
      
      MAP.LINES = DCOUNT(MAP, @FM)
      FOR M = 2 TO MAP.LINES
         fA = FIELD(MAP<M,1>, "_", 1)        ; * from Attribute
         fM = FIELD(MAP<M,1>, "_", 2)        ; * from Multi-value
         fS = FIELD(MAP<M,1>, "_", 3)        ; * from Sub-value
         
         CHK = fA[1,4]
         BEGIN CASE
            CASE "=CAT"
               GOSUB CAT..FIELDS             ; * return the SRC value
            CASE 1
               SRC = BASE.REC<fA,fM,fS>
         END CASE
         
         tA = FIELD(MAP<M,2>, "_", 1)        ; *   to Attribute
         tM = FIELD(MAP<M,2>, "_", 2)        ; *   to Multi-value
         tS = FIELD(MAP<M,2>, "_", 3)        ; *   to Sub-value
         
         iConv = MAP<M,3>                    ; *   to be developed
         oConv = MAP<M,4>                    ; *   to be developed
         
         OTHER.REC<tA,tM,tS> = SRC
      NEXT M
      
      FILE.ARR(1)= OTHERIO
      REC.ARR(1) = OTHER.REC
      ID.ARR(1)  = OTHER.ID
      WV.ARR(1)  = ""
      WU.ARR(1)  = ""
      
      CALL SR_DBWRITER_UV (RTN.STRING, MAT REC.ARR, MAT FILE.ARR, MAT ID.ARR, MAT WV.ARR, MAT WU.ARR)
      IF RTN.STRING THEN
         RTN.CODE = RTN.STRING
         RETURN
      END
      
      RTN.CODE = "200-OK"
      RETURN
      *
      * ============================================================================
      *
CAT..FIELDS:
      TEMP= CONVERT("}", "", MAP<M,1>)
      TEMP= CONVERT("{", @FM, TEMP)
      EOC = DCOUNT(TEMP, @FM)
      SRC = ""
      ** TEMP<1> will always be "=CAT"
      FOR C = 2 TO EOC
         DETS = TEMP<C>
         IF NOT(NUM(FIELD(DETS, "_", 1))) THEN
            DAT = DETS
         END ELSE
            a   = FIELD(DETS, "_", 1)
            m   = FIELD(DETS, "_", 2)
            s   = FIELD(DETS, "_", 3)
            DAT = BASE.REC<a,m,s>
         END
         SRC := DAT
      NEXT C
      RETURN
   END
