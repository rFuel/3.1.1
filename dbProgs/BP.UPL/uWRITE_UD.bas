      SUBROUTINE uWRITE_UD(ANS, DBT, LIST, LISTIO)
$INCLUDE I_Prologue
      IF DBT # "UD" THEN
         ANS = "UD writer called for DB Type ":DBT:" - FATAL"
      END ELSE
         ANS = ""
         OSBWRITE LIST ON LISTIO AT 0
         ERR = STATUS()
         BEGIN CASE
            CASE ERR=0
               ANS=""
            CASE ERR=1
               ANS = " LISTIO - Invalid file name"
            CASE ERR=2
               ANS = " LISTIO - File permission error"
            CASE ERR=4
               ANS = " LISTIO - does not exist."
            CASE 1
               ANS = " Unknown error on OSBWRITE"
         END CASE
      END
      RETURN
   END
