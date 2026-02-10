      SUBROUTINE SR.CLIENTBLOCK ( ERR, OLD, NEW )
$INCLUDE I_Prologue
      *
      *  Synopsis:
      *  07-10-20    a) CALLs must return newV, not use the record<a,m,s> !!!
      *
      * ----------------------------------------------------------------------
      ERR         = ""
      *
      RABCLIENT = "RAB.CLIENT"      ;  DEVCLIENT = "DEV.CLIENT"
      RABAML    = "RAB.CLIENT.AML"  ;  DEVAML    = "DEV.CLIENT.AML"
      * ------------------------------------------------------------
      OPEN RABCLIENT  TO RAB.CLIENT ELSE
         ERR = RABCLIENT:" open failure"
         GO END..PROG
      END
      OPEN RABAML    TO RAB.CLIENT.AML ELSE
         ERR = RABAML:" open failure"
         GO END..PROG
      END
      * ------------------------------------------------------------
      OPEN DEVCLIENT  TO DEV.CLIENT ELSE
         ERR = DEVCLIENT:" open failure"
         GO END..PROG
      END
      OPEN DEVAML  TO DEV.CLIENT.AML ELSE
         ERR = DEVAML:" open failure"
         GO END..PROG
      END
      * ------------------------------------------------------------
      READ OLDREC FROM RAB.CLIENT, OLD ELSE
         ERR = OLD:" is not in ":RABCLIENT
         GO END..PROG
      END
      READ OLDAML FROM RAB.CLIENT.AML, OLD ELSE
         ERR = OLD:" is not in ":RABAML
         GO END..PROG
      END
      NEWAML = OLDAML
***   NEWAML<2> = "aml-":RND(8)+1
      WRITE NEWAML ON DEV.CLIENT.AML, NEW
      SOME.NBR = RND(8)+1:RND(8)+1:RND(8)+1
      NEWREC = OLDREC
      NEWREC<1,1,1> = "lastName-":SOME.NBR
      NEWREC<7,1,1> = "FirstName":SOME.NBR:" SecondName":SOME.NBR
      NEWREC<72,1,1>= SOME.NBR:".email@mAiLserVer.cOm.au"
      NEWREC<9,1,1> = STR(RND(8)+1, 4):STR(RND(8)+1, 3):STR(RND(8)+1, 3)
      NEWREC<9,2,1> = STR(RND(8)+1, 4):STR(RND(8)+1, 3):STR(RND(8)+1, 3)
      NEWREC<78,1,1>= STR(RND(8)+1, 4):STR(RND(8)+1, 3):STR(RND(8)+1, 3)
      NEWREC<78,2,1>= STR(RND(8)+1, 4):STR(RND(8)+1, 3):STR(RND(8)+1, 3)
      NEWREC<78,3,1>= STR(RND(8)+1, 4):STR(RND(8)+1, 3):STR(RND(8)+1, 3)
      NEWREC<78,4,1>= STR(RND(8)+1, 4):STR(RND(8)+1, 3):STR(RND(8)+1, 3)
      NEWREC<78,5,1>= STR(RND(8)+1, 4):STR(RND(8)+1, 3):STR(RND(8)+1, 3)
      NEWREC<82,1,1>= STR(RND(8)+1, 4):STR(RND(8)+1, 3):STR(RND(8)+1, 3)
      newV = ""; CALL SR.SCRUBSTRING(NEWREC<2,1,1>, newV)  ; NEWREC<2,1,1> = newV
      newV = ""; CALL SR.SCRUBSTRING(NEWREC<3,1,1>, newV)  ; NEWREC<3,1,1> = newV
      newV = ""; CALL SR.SCRUBSTRING(NEWREC<2,2,1>, newV)  ; NEWREC<2,2,1> = newV
      newV = ""; CALL SR.SCRUBSTRING(NEWREC<3,2,1>, newV)  ; NEWREC<3,2,1> = newV
      *
      newV = ""; CALL SR.SCRUBSTRING(NEWREC<4,1,1>, newV)  ; NEWREC<4,1,1> = newV
      newV = ""; CALL SR.SCRUBSTRING(NEWREC<4,2,1>, newV)  ; NEWREC<4,2,1> = newV
      newV = ""; CALL SR.SCRUBNUMBER(NEWREC<5,1,1>, newV)  ; NEWREC<5,1,1> = newV
      newV = ""; CALL SR.SCRUBNUMBER(NEWREC<5,2,1>, newV)  ; NEWREC<5,2,1> = newV
      *
      newV = ""; CALL SR.SCRUBNUMBER(NEWREC<23,3,1>, newV) ; NEWREC<23,3,1> = newV
      newV = ""; CALL SR.SCRUBNUMBER(NEWREC<23,5,1>, newV) ; NEWREC<23,5,1> = newV
      * -----------------//
      newV = ""; CALL SR.SCRUBSTRING(NEWREC<2,3,1>, newV)  ; NEWREC<2,3,1> = newV
      newV = ""; CALL SR.SCRUBSTRING(NEWREC<3,3,1>, newV)  ; NEWREC<3,3,1> = newV
      newV = ""; CALL SR.SCRUBSTRING(NEWREC<6,1,1>, newV)  ; NEWREC<6,1,1> = newV
      
      newV = ""; CALL SR.SCRUBSTRING(NEWREC<11,1,1>, newV) ; NEWREC<11,1,1> = newV
      newV = ""; CALL SR.SCRUBSTRING(NEWREC<12,1,1>, newV) ; NEWREC<12,1,1> = newV
      newV = ""; CALL SR.SCRUBSTRING(NEWREC<12,2,1>, newV) ; NEWREC<12,2,1> = newV
      newV = ""; CALL SR.SCRUBSTRING(NEWREC<12,3,1>, newV) ; NEWREC<12.3,1> = newV
      newV = ""; CALL SR.SCRUBSTRING(NEWREC<21,1,1>, newV) ; NEWREC<21,1,1> = newV
      newV = ""; CALL SR.SCRUBNUMBER(NEWREC<22,2,1>, newV) ; NEWREC<22,2,1> = newV
      newV = ""; CALL SR.SCRUBNUMBER(NEWREC<22,3>  , newV) ; NEWREC<22,3>   = newV
      newV = ""; CALL SR.SCRUBNUMBER(NEWREC<24>, newV)     ; NEWREC<24> = newV
      newV = ""; CALL SR.SCRUBNUMBER(NEWREC<27>, newV)     ; NEWREC<27> = newV
      newV = ""; CALL SR.SCRUBNUMBER(NEWREC<32>, newV)     ; NEWREC<32> = newV
      newV = ""; CALL SR.SCRUBSTRING(NEWREC<35>, newV)     ; NEWREC<35> = newV
      newV = ""; CALL SR.SCRUBNUMBER(NEWREC<35>, newV)     ; NEWREC<35> = newV
      newV = ""; CALL SR.SCRUBSTRING(NEWREC<38>, newV)     ; NEWREC<38> = newV
      newV = ""; CALL SR.SCRUBNUMBER(NEWREC<42>, newV)     ; NEWREC<42> = newV
      newV = ""; CALL SR.SCRUBSTRING(NEWREC<42>, newV)     ; NEWREC<42> = newV
      newV = ""; CALL SR.SCRUBNUMBER(NEWREC<44>, newV)     ; NEWREC<44> = newV
      newV = ""; CALL SR.SCRUBSTRING(NEWREC<44>, newV)     ; NEWREC<44> = newV
      newV = ""; CALL SR.SCRUBNUMBER(NEWREC<46>, newV)     ; NEWREC<46> = newV
      newV = ""; CALL SR.SCRUBNUMBER(NEWREC<48>, newV)     ; NEWREC<48> = newV
      newV = ""; CALL SR.SCRUBSTRING(NEWREC<48>, newV)     ; NEWREC<48> = newV
      newV = ""; CALL SR.SCRUBSTRING(NEWREC<54>, newV)     ; NEWREC<54> = newV
      newV = ""; CALL SR.SCRUBNUMBER(NEWREC<54>, newV)     ; NEWREC<54> = newV
      newV = ""; CALL SR.SCRUBNUMBER(NEWREC<56>, newV)     ; NEWREC<56> = newV
      newV = ""; CALL SR.SCRUBSTRING(NEWREC<68>, newV)     ; NEWREC<68> = newV
      newV = ""; CALL SR.SCRUBSTRING(NEWREC<70>, newV)     ; NEWREC<70> = newV
      newV = ""; CALL SR.SCRUBSTRING(NEWREC<81>, newV)     ; NEWREC<81> = newV
      newV = ""; CALL SR.SCRUBSTRING(NEWREC<93>, newV)     ; NEWREC<93> = newV
      newV = ""; CALL SR.SCRUBNUMBER(NEWREC<93>, newV)     ; NEWREC<93> = newV
      newV = ""; CALL SR.SCRUBNUMBER(NEWREC<94>, newV)     ; NEWREC<94> = newV
      newV = ""; CALL SR.SCRUBSTRING(NEWREC<135>, newV)    ; NEWREC<135>= newV
      newV = ""; CALL SR.SCRUBSTRING(NEWREC<136>, newV)    ; NEWREC<136>= newV
      newV = ""; CALL SR.SCRUBNUMBER(NEWREC<138>, newV)    ; NEWREC<138>= newV
      newV = ""; CALL SR.SCRUBSTRING(NEWREC<139>, newV)    ; NEWREC<139>= newV
      newV = ""; CALL SR.SCRUBNUMBER(NEWREC<140>, newV)    ; NEWREC<140>= newV
      newV = ""; CALL SR.SCRUBNUMBER(NEWREC<143>, newV)    ; NEWREC<143>= newV
      newV = ""; CALL SR.SCRUBNUMBER(NEWREC<171>, newV)    ; NEWREC<171>= newV
      newV = ""; CALL SR.SCRUBNUMBER(NEWREC<172>, newV)    ; NEWREC<172>= newV
      newV = ""; CALL SR.SCRUBSTRING(NEWREC<173>, newV)    ; NEWREC<173>= newV
      newV = ""; CALL SR.SCRUBSTRING(NEWREC<174>, newV)    ; NEWREC<174>= newV
      newV = ""; CALL SR.SCRUBNUMBER(NEWREC<174>, newV)    ; NEWREC<174>= newV
      * -----------------//
      WRITE NEWREC ON DEV.CLIENT, NEW
      *
END..PROG:
      IF ERR # "" THEN CRT "SR.CLIENTBLOCK   ":ERR
      RETURN
   END

