$INCLUDE I_Prologue
      PROMPT ""
      DUMMY = @(0,0)
      FOR I = 1 TO 5
         PRINT
      NEXT I
      PRINT STR("-", 79)
      PRINT "NB: Everything IS case sensitive !"
      PRINT STR("-", 79)
      OPEN "uDIR"  TO uDIR ELSE STOP "Cannot open uDIR !!"
      TEMPLATE = ""
10    PRINT; PRINT 'u2File=':; INPUT SRC.FILE
      IF SRC.FILE="" OR SRC.FILE="Q" THEN STOP
      OPEN SRC.FILE TO CHECKIO ELSE
         PRINT "   no such file"
         GO 10
      END
      TEMPLATE<1> = "u2File=":SRC.FILE
      
20    PRINT; PRINT 'select=#LIST ':; INPUT SEL.NAME
      IF SEL.NAME = "" OR SEL.NAME = "Q" THEN
         PRINT; PRINT; PRINT
         GO 10
      END
      TEMPLATE<2> = 'select=#LIST ':SEL.NAME
      
30    PRINT; PRINT 'nselect=':; INPUT NSEL.NAME
      IF NSEL.NAME = "Q" THEN
         PRINT; PRINT; PRINT
         GO 10
      END
      TEMPLATE<3> = 'nselect=':NSEL.NAME
      
40    PRINT; PRINT 'sqlTable=':; INPUT SQL.NAME
      IF SQL.NAME = "" OR SQL.NAME = "Q" THEN
         PRINT; PRINT; PRINT
         GO 10
      END
      TEMPLATE<4> = 'sqlTable=':SQL.NAME
      
50    PRINT; PRINT 'list=':; INPUT CSV.LIST
      IF CSV.LIST = "" OR CSV.LIST = "Q" THEN
         PRINT; PRINT; PRINT
         GO 10
      END
      TEMPLATE<5> = 'list=':CSV.LIST
      
60    PRINT; PRINT 'proceed=':; ; INPUT PROCEED
      IF PROCEED = "" OR PROCEED = "Q" THEN
         PRINT; PRINT; PRINT
         GO 10
      END
      IF PROCEED # "true" AND PROCEED # "false" THEN
         PRINT "   invalid answer - true or false only"
         GO 60
      END
      TEMPLATE<6> = 'proceed=':PROCEED
      
70    PRINT; PRINT 'colpfx=':; ; INPUT COL.PFX
      IF COL.PFX = "" OR COL.PFX = "Q" THEN
         PRINT; PRINT; PRINT
         GO 10
      END
      TEMPLATE<7> = 'colpfx=':COL.PFX
      
100   PRINT; PRINT "Save as (Excluding '.prt') ":; INPUT PRT.FILENAME
      IF PRT.FILENAME="" OR PRT.FILENAME="Q" THEN
         PRINT; PRINT; PRINT
         GO 10
      END
      
110   PRINT; PRINT "Number of prt files ":; INPUT NBR.FILES
      IF NBR.FILES = "" OR NOT(NUM(NBR.FILES)) THEN
         PRINT "   invalid answer - integer only"
         GO 110
      END
      
120   PRINT; PRINT "Place these in grp name ":; INPUT GRP.FILENAME
      IF GRP.FILENAME="" OR GRP.FILENAME="Q" THEN
         PRINT; PRINT; PRINT
         GO 10
      END
      
      PRINT
      PRINT STR("-", 79)
      EOI = DCOUNT(TEMPLATE, @FM)
      PRINT "Create ":NBR.FILES:"   ":PRT.FILENAME:"nnn.prt files ":
      PRINT "and place them in ":GRP.FILENAME
      PRINT
      FOR I = 1 TO EOI
         PRINT I "R#2":": ":TEMPLATE<I>
      NEXT I
      PRINT
      PRINT "Okay to proceed (Y/n) ":; INPUT ANS
      IF ANS="" THEN ANS = "Y"
      IF ANS#"Y" THEN
         PRINT "... no action taken."
         STOP
      END
      
      GRP.ITEM = "maps=":@FM:"presql=":@FM:"preuni="
      
      FOR I = 1 TO NBR.FILES
         IF I > 1 THEN GRP.ITEM<1> := ","
         FNAME = PRT.FILENAME:"_P":I:".prt"
         tITEM = TEMPLATE
         tITEM<2> := ("000":I) "R#3"
         WRITE tITEM ON uDIR, FNAME
         GRP.ITEM<1> := FNAME
      NEXT I
      WRITE GRP.ITEM ON uDIR, GRP.FILENAME
      PRINT
      PRINT "Done - items are in uDIR"
      PRINT
      STOP
   END
