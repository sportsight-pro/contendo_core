WITH
  statsCountersTable AS(
  SELECT
    FLOOR(questionsCount/{NumQuestionsToSelect}) AS nQuestionsToSelect,
    FLOOR(questionsCount/{NumQuestionsToSelect}*RAND()) AS questionToSelect,
    *
  FROM
    `Sportsight_Stats.all_questions_questioncode_stats`
  WHERE
    ContentDefCode = '{ContentDefCode}'
  ),
  questions AS (
  SELECT
    questionsTable.*,
    statsCountersTable.nQuestionsToSelect,
    statsCountersTable.questionToSelect
  FROM
    `temp.questions_{ContentDefCode}` questionsTable
  LEFT JOIN
    statsCountersTable
  ON
    questionsTable.ContentDefCode = statsCountersTable.ContentDefCode
    AND questionsTable.QuestionCode = statsCountersTable.QuestionCode
    AND questionsTable.StatObject = statsCountersTable.StatObject),
  questionsFinal AS (
  SELECT
    IF (nQuestionsToSelect=0, TRUE,
    IF (MOD(questionCodeRow,CAST(nQuestionsToSelect AS int64))=questionToSelect, TRUE, FALSE)) AS selected,
    *
  FROM
    questions)
SELECT
  *
FROM
  questionsFinal
WHERE
  selected
ORDER BY
  slotNum, questionScore
