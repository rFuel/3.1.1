      SUBROUTINE rTRIGGER(ITEM)
      * -----------------------------------------------------
      COMMON /uTRIGGERS/ TFILE
      * -----------------------------------------------------
      IF UNASSIGNED(TFILE) THEN OPEN 'TRIGGERS' TO TFILE ELSE RETURN
      KEY = Event[1,1]:"_":u2File
      * Examples:             
      * I_{file} insert events
      * U_{file} update events
      * D_{file} delete events
      READ CALL.LIST FROM TRIGGERS, KEY ELSE RETURN
      EOI = DCOUNT(CALL.LIST, @FM)
      FOR I = 1 TO EOI
         NAME = CALL.LIST<I,1,1>
         SUBR  = CALL.LIST<I,2,1>
***      IF NAME = "" THEN NAME = TrigName
         IF SUBR = "" THEN CONTINUE
         CALL @SUBR(ITEM)
      NEXT I
      RETURN
      * ------------------------------------------------
   END
