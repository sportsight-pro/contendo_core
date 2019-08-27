twoQuestions AS (
  SELECT
    #COUNT(*) OVER (rows between unbounded preceding and unbounded following) AS totalQuestionsCount,
    #
    # Calculate the question score base on higher rank and value differences relative to value-range.
    s2.internalDenseRank-s1.internalDenseRank AS rankDiff,
    ROUND((1-ABS(s1.StatValue-s2.StatValue)/s2.valueRange)*100, 3) +
     IF (s1.internalDenseRank<s2.numRanks*0.2, s1.internalDenseRank/s2.numRanks*350, 70+s1.internalDenseRank/s2.numRanks*30)
    AS questionScore,
    s1.StatObject,
    s1.Code,
    s1.StatRollingDays,
    s1.StatDate,
    s1.QuestionCode,
    s1.startSlot,
    (
    SELECT
      AS STRUCT s1.*) AS Stat1,
    (
    SELECT
      AS STRUCT s2.*) AS Stat2
  FROM
    insightStatsFinal s1
  LEFT JOIN
    insightStatsFinal s2
  ON
    s1.QuestionCode=s2.QuestionCode
    AND s1.internalDenseRank < s2.internalDenseRank
    AND s1.StatObject = s2.StatObject
    AND s1.StatRollingDays = s2.StatRollingDays
    AND s1.StatDate = s2.StatDate
  WHERE
    # match was not found
    s2.StatName IS NOT NULL
    #
    # Filter out questions with
    AND (ABS(s2.StatValue-s2.minValue+1)/(s2.valueRange) > 0.1 OR s2.numRanks<6) ),
  twoQuestionsNumbered AS (
  SELECT
    #ROW_NUMBER() OVER () AS rowNum,
    ROW_NUMBER() OVER (PARTITION BY StatObject, StatRollingDays, StatDate, QuestionCode ORDER BY questionScore) AS questionCodeRow,
    COUNT(*) OVER (PARTITION BY StatObject, StatRollingDays, StatDate, QuestionCode) AS questionCodeCount,
    *
  FROM
    twoQuestions
  WHERE
    TRUE
    AND {QuestionsFilter}
    ),
    twoQuestionsFinal as (
    SELECT
      '{ContentDefCode}' as ContentDefCode,
      CEIL(questionCodeRow/GREATEST(CEIL(questionCodeCount/({NumSlots}-startSlot+1)),2.0))+startSlot-1 as slotNum,
      *
    FROM
      twoQuestionsNumbered)