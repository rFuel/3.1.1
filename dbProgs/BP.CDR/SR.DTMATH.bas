      SUBROUTINE SR.DTMATH (ERR, DATETIME, OPER, INTERVAL, PERIOD, RESULT)
$INCLUDE I_Prologue
      *
      IF DATETIME = "" THEN GO END..SRTN  ;* DATE() : _ : TIME() ... or "NOW"
      IF OPER     = "" THEN GO END..SRTN  ;* ADD, SUB or MUL
      IF INTERVAL = "" THEN GO END..SRTN  ;* integer 
      IF PERIOD   = "" THEN GO END..SRTN  ;* smhDMY 
      ERR = ""
      RESULT = ""
      *
      IF UPCASE(DATETIME) = "NOW" THEN
         DTE = DATE()
         TIM = TIME()
      END ELSE
         DTE = FIELD(DATETIME, "_", 1)
         TIM = FIELD(DATETIME, "_", 2)
      END
      IF DTE = "" THEN GO END..SRTN
      IF TIM = "" THEN GO END..SRTN
      IF NOT(INDEX(" ADD SUB MUL ", OPER, 1)) THEN GO END..SRTN
      IF NOT(NUM(INTERVAL)) OR INTERVAL=""    THEN GO END..SRTN
      IF NOT(INDEX(" smhDMY ", PERIOD, 1))    THEN GO END..SRTN
      *
      D.RESULT = "" ; T.RESULT = ""
      IF UPCASE(PERIOD) = PERIOD THEN GOSUB HANDLE..DATE ELSE GOSUB HANDLE..TIME
      *
      IF D.RESULT # "" THEN DTE = D.RESULT
      IF T.RESULT # "" THEN TIM = T.RESULT
      RESULT = DTE:"_":TIM
      * ------------------------------------------------------------------
END..SRTN:
      RETURN
      * ------------------------------------------------------------------
HANDLE..DATE:
      TMP = EREPLACE(OCONV(DTE, "D4:"), ":", @FM)
      DAY = TMP<1>
      MTH = TMP<2>
      YYR = TMP<3>
      *
      MAX.DAY.PARAMS = "01 ":(MTH + 1):" ":YYR
      GOSUB CALC..MAXDAY   ;* creates the variable MAX.DAY
      COMPONENT = 0
      BEGIN CASE
         CASE PERIOD = "D"
            COMPONENT = DAY
         CASE PERIOD = "M"
            COMPONENT = MTH
         CASE PERIOD = "Y"
            COMPONENT = YYR
      END CASE
      BEGIN CASE
         CASE OPER = "ADD"
            ANS = COMPONENT + INTERVAL
         CASE OPER = "SUB"
            ANS = COMPONENT - INTERVAL
         CASE OPER = "MUL"
            ANS = COMPONENT * INTERVAL
      END CASE
      BEGIN CASE
         CASE PERIOD = "D"
            DAY = ANS
         CASE PERIOD = "M"
            MTH = ANS
         CASE PERIOD = "Y"
            YYR = ANS
      END CASE
      LOOP WHILE DAY > MAX.DAY DO
         DAY = DAY - MAX.DAY
         MTH+= 1
         IF MTH > 12 THEN MTH = MTH - 12; YYR +=1
         MAX.DAY.PARAMS = "01 ":(MTH + 1):" ":YYR
         GOSUB CALC..MAXDAY   ;* creates the variable MAX.DAY
      REPEAT
      LOOP WHILE MTH > 12 DO
         MTH = MTH - 12
         YYR+= 1
      REPEAT
      D.RESULT = ICONV(DAY:":":MTH:":":YYR, "D4:")
      RETURN
      * ------------------------------------------------------------------
CALC..MAXDAY:
      PARAMS = EREPLACE(MAX.DAY.PARAMS, " ", @FM)
      DD = PARAMS<1>
      MM = PARAMS<2>
      YY = PARAMS<3>
      LM = MM - 1
      YR = YY
      LOOP WHILE MM > 12 DO
         MM = MM - 12
         YR+= 1
      REPEAT
      D1 = ICONV("01:" : LM: ":" : YY, "D4:")
      D2 = ICONV("01:" : MM: ":" : YR, "D4:")
      MAX.DAY = ( D2 - D1 )
      RETURN
      * ------------------------------------------------------------------
HANDLE..TIME:
      TMP = EREPLACE(OCONV(TIM, "MTS"), ":", @FM)
      HRS = TMP<1>
      MIN = TMP<2>
      SEC = TMP<3>
      DAY = 0
      COMPONENT = 0
      BEGIN CASE
         CASE PERIOD = "s"
            COMPONENT = SEC
         CASE PERIOD = "m"
            COMPONENT = MIN
         CASE PERIOD = "h"
            COMPONENT = HRS
      END CASE
      BEGIN CASE
         CASE OPER = "ADD"
            ANS = COMPONENT + INTERVAL
         CASE OPER = "SUB"
            ANS = COMPONENT - INTERVAL
         CASE OPER = "MUL"
            ANS = COMPONENT * INTERVAL
      END CASE
      BEGIN CASE
         CASE PERIOD = "s"
            SEC = ANS
         CASE PERIOD = "m"
            MIN = ANS
         CASE PERIOD = "h"
            HRS = ANS
      END CASE
      *
      LOOP WHILE SEC > 60 DO
         VALUE = SEC / 60
         EXT = INT(VALUE)
         DEN = VALUE - EXT
         MIN += EXT
         SEC = INT(60 * DEN)
      REPEAT
      LOOP WHILE MIN > 60 DO
         VALUE = MIN / 60
         EXT = INT(VALUE)
         DEN = VALUE - EXT
         HRS += EXT
         MIN = INT(60 * DEN)
      REPEAT
      LOOP WHILE HRS > 24 DO
         VALUE = HRS / 60
         EXT = INT(VALUE)
         DEN = VALUE - EXT
         DAY += EXT
         HRS = INT(24 * DEN)
      REPEAT
      T.RESULT = ICONV(HRS:":":MIN:":":SEC, "MTS")
      IF DAY # 0 THEN 
         INTERVAL = DAY
         OPER     = "ADD"
         PERIOD   = "D"
         GOSUB HANDLE..DATE
      END
      RETURN
      * ------------------------------------------------------------------
   END

